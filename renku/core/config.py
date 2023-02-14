# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Configuration management."""

import configparser
import os
from io import StringIO

from renku.core.constant import DATA_DIR_CONFIG_KEY
from renku.domain_model.enums import ConfigFilter
from renku.domain_model.project_context import project_context


def global_config_read_lock():
    """Create a user-level config read lock."""
    from renku.core.util.contexts import Lock

    return Lock(project_context.global_config_path)


def global_config_write_lock():
    """Create a user-level config write lock."""
    from renku.core.util.contexts import Lock

    return Lock(project_context.global_config_path, mode="exclusive")


def get_value(section, key, config_filter=ConfigFilter.ALL):
    """Get value from specified section and key."""
    config = load_config(config_filter=config_filter)
    return config.get(section, key, fallback=None)


def set_value(section, key, value, global_only=False):
    """Set value to specified section and key."""
    config_filter = ConfigFilter.GLOBAL_ONLY

    if not global_only:
        config_filter = ConfigFilter.LOCAL_ONLY
        _check_config_is_not_readonly(section, key)

    config = load_config(config_filter=config_filter)
    if section in config:
        config[section][key] = value
    else:
        config[section] = {key: value}

    store_config(config, global_only=global_only)


def remove_value(section, key, global_only=False):
    """Remove key from specified section or remove sections."""
    config_filter = ConfigFilter.GLOBAL_ONLY

    if not global_only:
        config_filter = ConfigFilter.LOCAL_ONLY
        _check_config_is_not_readonly(section, key)

    value = None
    config = load_config(config_filter=config_filter)
    removed_sections = []
    if section in config:
        if key == "*":
            value = config.pop(section)
        else:
            value = config[section].pop(key, None)

            if not config[section].keys():
                removed_sections.append(section)
    elif section.endswith("*"):
        section_prefix = section[:-1]
        for section in config:
            if section.startswith(section_prefix):
                value = config[section]
                removed_sections.append(section)

    for section in removed_sections:
        config.pop(section)
    if value is not None:
        store_config(config, global_only=global_only)
    return value


def load_config(config_filter=ConfigFilter.ALL):
    """Loads local, global or both configuration object."""
    try:
        import importlib_resources  # type:ignore
    except ImportError:
        import importlib.resources as importlib_resources  # type:ignore

    # NOTE: Use RawConfigParser because ConfigParser does non-standard INI interpolation of some values
    config = configparser.RawConfigParser()
    ref = importlib_resources.files("renku.data") / "defaults.ini"
    with importlib_resources.as_file(ref) as default_ini:
        config_files = [default_ini]

    if config_filter == ConfigFilter.LOCAL_ONLY:
        config_files += [project_context.local_config_path]
    elif config_filter == ConfigFilter.GLOBAL_ONLY:
        config_files += [project_context.global_config_path]
    elif config_filter == ConfigFilter.ALL:
        config_files += [project_context.global_config_path, project_context.local_config_path]

    if config_filter != ConfigFilter.LOCAL_ONLY:
        with global_config_read_lock():
            config.read(config_files)
    else:
        config.read(config_files)

    # NOTE: transform config section for backwards compatibility. Changes section names like
    # 'renku "interactive"' to just 'interactive' to be in line with python config conventions.
    for section in config.sections():
        if not section.startswith('renku "'):
            continue

        config[section[7:-1]] = dict(config.items(section))  # NOTE: Drop first 7 and last char
        config.pop(section)

    return config


def store_config(config, global_only):
    """Persists locally or globally configuration object.

    Global configuration is updated only when :global_only: is True,
    otherwise, updates are written to local project configuration
    """
    filepath = project_context.global_config_path if global_only else project_context.local_config_path

    if global_only:
        with global_config_write_lock():
            os.umask(0)
            fd = os.open(filepath, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
            write_config(fd, config)
    else:
        write_config(filepath, config)


def get_config(config_filter=ConfigFilter.ALL, as_string=True):
    """Read all configurations."""
    config = load_config(config_filter=config_filter)
    if as_string:
        with StringIO() as output:
            config.write(output)
            return output.getvalue()
    else:
        return {f"{s}.{k}": v for s in config.sections() for k, v in config.items(s)}


def _check_config_is_not_readonly(section, key):
    from renku.core import errors

    readonly_configs = {"renku": [DATA_DIR_CONFIG_KEY]}

    value = get_value(section, key, config_filter=ConfigFilter.LOCAL_ONLY)
    if not value:
        return

    if key in readonly_configs.get(section, []):
        raise errors.ParameterError(f"Configuration {key} cannot be modified.")


def write_config(filepath, config):
    """Write config value to a specified path."""
    with open(filepath, "w+") as file:
        config.write(file)
