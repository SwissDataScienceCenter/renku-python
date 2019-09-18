# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
import fcntl
import os
from pathlib import Path

import attr
import click

APP_NAME = 'Renku'
"""Application name for storing configuration."""

RENKU_HOME = '.renku'
"""Project directory name."""


def print_app_config_path(ctx, param, value):
    """Print application config path."""
    if not value or ctx.resilient_parsing:
        return
    click.echo(ConfigManagerMixin().config_path)
    ctx.exit()


def default_config_dir():
    """Return default config directory."""
    return click.get_app_dir(APP_NAME, force_posix=True)


@attr.s
class ConfigManagerMixin:
    """Client for handling global configuration."""

    config_dir = attr.ib(default=default_config_dir(), converter=str)
    config_name = attr.ib(default='renku.ini', converter=str)

    _lock = attr.ib(default=None)

    def __enter__(self):
        """Acquire a lock file."""
        lock_name = '{0}/{1}.lock'.format(self.config_dir, self.config_name)
        locked_file_descriptor = open(lock_name, 'w+')
        fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
        self._lock = locked_file_descriptor

    def __exit__(self, type, value, traceback):
        """Release lock file."""
        self._lock.close()

    @property
    def config_path(self):
        """Renku config path."""
        config = Path(self.config_dir)
        if not config.exists():
            config.mkdir()

        return config / Path(self.config_name)

    def load_config(self):
        """Loads global configuration object."""
        config = configparser.ConfigParser()
        config.read(str(self.config_path))
        return config

    def store_config(self, config):
        """Persists global configuration object."""
        os.umask(0)
        fd = os.open(
            str(self.config_path), os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600
        )

        with open(fd, 'w+') as file:
            config.write(file)

        return self.load_config()

    def get_value(self, section, key):
        """Get value from specified section and key."""
        config = self.load_config()
        return config.get(section, key, fallback=None)

    def set_value(self, section, key, value):
        """Set value to specified section and key."""
        config = self.load_config()
        if section in config:
            config[section][key] = value
        else:
            config[section] = {key: value}

        config = self.store_config(config)
        return config

    def remove_value(self, section, key):
        """Remove key from specified section."""
        config = self.load_config()

        if section in config:
            config[section].pop(key)

            if not config[section].keys():
                config.pop(section)

        config = self.store_config(config)
        return config


def get_config(client, write_op, is_global):
    """Get configuration object."""
    if is_global:
        return client

    if write_op:
        return client.repo.config_writer()
    return client.repo.config_reader()
