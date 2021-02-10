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
"""Client for handling a configuration."""
import configparser
import os
from io import StringIO
from pathlib import Path

import attr
import click
import filelock
from pkg_resources import resource_filename

from renku.core.models.enums import ConfigFilter

APP_NAME = "Renku"
"""Application name for storing configuration."""

RENKU_HOME = ".renku"
"""Project directory name."""


def _get_global_config_dir():
    """Return user's config directory."""
    return click.get_app_dir(APP_NAME, force_posix=True)


@attr.s
class ConfigManagerMixin:
    """Client for handling global configuration."""

    CONFIG_NAME = "renku.ini"

    DATA_DIR_CONFIG_KEY = "data_directory"

    _global_config_dir = _get_global_config_dir()

    @property
    def global_config_dir(self):
        """Return user's config directory."""
        return self._global_config_dir

    @property
    def global_config_path(self):
        """Renku global (user's) config path."""
        config = Path(self.global_config_dir)
        if not config.exists():
            config.mkdir()

        return str(config / Path(self.CONFIG_NAME))

    @property
    def local_config_path(self):
        """Renku local (project) config path."""
        return str(self.renku_path / self.CONFIG_NAME)

    @property
    def global_config_lock(self):
        """Create a user-level config lock."""
        lock_file = f"{self.global_config_path}.lock"

        import logging

        logging.getLogger("filelock").setLevel(logging.ERROR)

        return filelock.FileLock(lock_file, timeout=0)

    def load_config(self, config_filter=ConfigFilter.ALL):
        """Loads local, global or both configuration object."""
        config = configparser.ConfigParser()
        config_files = [resource_filename("renku", "data/defaults.ini")]

        if config_filter == ConfigFilter.LOCAL_ONLY:
            config_files += [self.local_config_path]
        elif config_filter == ConfigFilter.GLOBAL_ONLY:
            config_files += [self.global_config_path]
        elif config_filter == ConfigFilter.ALL:
            config_files += [self.global_config_path, self.local_config_path]

        if config_filter != ConfigFilter.LOCAL_ONLY:
            with self.global_config_lock:
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

    def store_config(self, config, global_only):
        """Persists locally or globally configuration object.

        Global configuration is updated only when :global_only: is True,
        otherwise, updates are written to local project configuration
        """
        filepath = self.global_config_path if global_only else self.local_config_path

        if global_only:
            os.umask(0)
            fd = os.open(filepath, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
            with self.global_config_lock:
                with open(fd, "w+") as file:
                    config.write(file)
        else:
            with open(filepath, "w+") as file:
                config.write(file)

    def get_config(self, config_filter=ConfigFilter.ALL, as_string=True):
        """Read all configurations."""
        config = self.load_config(config_filter=config_filter)
        if as_string:
            with StringIO() as output:
                config.write(output)
                return output.getvalue()
        else:
            return {f"{s}.{k}": v for s in config.sections() for k, v in config.items(s)}

    def get_value(self, section, key, config_filter=ConfigFilter.ALL):
        """Get value from specified section and key."""
        config = self.load_config(config_filter=config_filter)
        return config.get(section, key, fallback=None)

    def set_value(self, section, key, value, global_only=False):
        """Set value to specified section and key."""
        config_filter = ConfigFilter.GLOBAL_ONLY

        if not global_only:
            config_filter = ConfigFilter.LOCAL_ONLY
            self._check_config_is_not_readonly(section, key)

        config = self.load_config(config_filter=config_filter)
        if section in config:
            config[section][key] = value
        else:
            config[section] = {key: value}

        self.store_config(config, global_only=global_only)

    def remove_value(self, section, key, global_only=False):
        """Remove key from specified section."""
        config_filter = ConfigFilter.GLOBAL_ONLY

        if not global_only:
            config_filter = ConfigFilter.LOCAL_ONLY
            self._check_config_is_not_readonly(section, key)

        config = self.load_config(config_filter=config_filter)
        if section in config:
            value = config[section].pop(key, None)

            if not config[section].keys():
                config.pop(section)

            self.store_config(config, global_only=global_only)
            return value

    def _check_config_is_not_readonly(self, section, key):
        from renku.core import errors

        readonly_configs = {"renku": [self.DATA_DIR_CONFIG_KEY]}

        value = self.get_value(section, key, config_filter=ConfigFilter.LOCAL_ONLY)
        if not value:
            return

        if key in readonly_configs.get(section, []):
            raise errors.ParameterError(f"Configuration {key} cannot be modified.")


CONFIG_LOCAL_PATH = [Path(RENKU_HOME) / ConfigManagerMixin.CONFIG_NAME]
