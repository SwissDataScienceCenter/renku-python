# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
from renku.core import errors
from renku.core.incubation.command import Command
from renku.core.management.config import CONFIG_LOCAL_PATH
from renku.core.models.enums import ConfigFilter


def _split_section_and_key(key):
    """Return a tuple with config section and key."""
    parts = key.split(".")
    if len(parts) > 1:
        return "{0}".format(parts[0]), ".".join(parts[1:])
    return "renku", key


def _update_multiple_config(client, values, global_only=False, commit_message=None):
    """Add, update, or remove multiple configuration values."""
    for k, v in values.items():
        if v is not None:
            _update_config(client, k, value=v, global_only=global_only)
        else:
            _update_config(client, k, remove=True, global_only=global_only)


def update_multiple_config():
    """Command for updating config."""
    return (
        Command()
        .command(_update_multiple_config)
        .require_migration()
        .with_commit(commit_if_empty=False, commit_only=CONFIG_LOCAL_PATH)
    )


def _update_config(client, key, *, value=None, remove=False, global_only=False, commit_message=None):
    """Add, update, or remove configuration values."""
    section, section_key = _split_section_and_key(key)
    if remove:
        value = client.remove_value(section, section_key, global_only=global_only)
        if value is None:
            raise errors.ParameterError('Key "{}" not found.'.format(key))
    else:
        client.set_value(section, section_key, value, global_only=global_only)
        return value


def update_config():
    """Command for updating config."""
    return (
        Command()
        .command(_update_config)
        .require_migration()
        .with_commit(commit_if_empty=False, commit_only=CONFIG_LOCAL_PATH)
    )


def _read_config(client, key, config_filter=ConfigFilter.ALL, as_string=True):
    """Read configuration."""
    if key:
        section, section_key = _split_section_and_key(key)
        value = client.get_value(section, section_key, config_filter=config_filter)
        if value is None:
            raise errors.ParameterError('Key "{}" not found.'.format(key))
        return value

    return client.get_config(config_filter=config_filter, as_string=as_string)


def read_config():
    """Command for updating config."""
    return Command().command(_read_config)
