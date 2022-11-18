# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Get and set Renku repository or global options."""

from typing import Dict, Optional

from pydantic import validate_arguments

from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.config import get_config, get_value, remove_value, set_value
from renku.core.constant import CONFIG_LOCAL_PATH
from renku.domain_model.enums import ConfigFilter


def _split_section_and_key(key):
    """Return a tuple with config section and key.

    Args:
        key: The config key.

    Returns:
        Tuple of "renku" and the supplied key.
    """
    parts = key.split(".")
    if len(parts) > 1:
        return "{0}".format(parts[0]), ".".join(parts[1:])
    return "renku", key


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _update_multiple_config(
    values: Dict[str, Optional[str]], global_only: bool = False, commit_message: Optional[str] = None
):
    """Add, update, or remove multiple configuration values.

    Args:
        values(Dict[str, str]): Dictionary of config key values to update.
        global_only(bool): Whether to only update global config (Default value = False).
        commit_message(Optional[str]): Commit message for change (Default value = None).
    """
    for k, v in values.items():
        if v is not None:
            _update_config(k, value=v, global_only=global_only)
        else:
            _update_config(k, remove=True, global_only=global_only)


def update_multiple_config():
    """Command for updating config."""
    return (
        Command()
        .command(_update_multiple_config)
        .with_database()
        .require_migration()
        .with_commit(commit_if_empty=False, commit_only=[CONFIG_LOCAL_PATH])
    )


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _update_config(
    key: str,
    *,
    value: Optional[str] = None,
    remove: bool = False,
    global_only: bool = False,
    commit_message: Optional[str] = None,
):
    """Add, update, or remove configuration values.

    Args:
        key(str): Config key.
        value(Optional[str]): Config value (Default value = None).
        remove(bool): Whether to remove values (Default value = False).
        global_only(bool): Whether to only update global config (Default value = False).
        commit_message(Optional[str]): Commit message for change (Default value = None).
    Raises:
        errors.ParameterError: If key wasn't found.
    Returns:
        The modified/removed value.
    """
    section, section_key = _split_section_and_key(key)
    if remove:
        value = remove_value(section, section_key, global_only=global_only)
        if value is None:
            raise errors.ParameterError('Key "{}" not found.'.format(key))
    else:
        set_value(section, section_key, value, global_only=global_only)
        return value


def update_config():
    """Command for updating config."""
    return (
        Command()
        .command(_update_config)
        .require_migration()
        .with_commit(commit_if_empty=False, commit_only=[CONFIG_LOCAL_PATH], skip_staging=True)
        .with_database()
    )


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _read_config(key: Optional[str], config_filter: ConfigFilter = ConfigFilter.ALL, as_string: bool = True):
    """Read configuration.

    Args:
        key: Config key.
        config_filter: Which config files to read from (Default value = ConfigFilter.ALL).
        as_string: Whether to return a string or dictionary (Default value = True).

    Returns:
        String or dictionary containing configuration values.
    """
    if key:
        section, section_key = _split_section_and_key(key)
        value = get_value(section, section_key, config_filter=config_filter)
        if value is None:
            raise errors.ParameterError(f"Key '{key}' not found.")
        return value

    return get_config(config_filter=config_filter, as_string=as_string)


def read_config():
    """Command for updating config."""
    return Command().command(_read_config)
