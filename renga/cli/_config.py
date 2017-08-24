# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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
"""Configuration utilities."""

import errno
import os
from functools import update_wrapper

import click
import yaml

APP_NAME = 'Renga'


def config_path():
    """Return config path."""
    directory = click.get_app_dir(APP_NAME, force_posix=True)
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return os.path.join(directory, 'config.yml')


def read_config():
    """Read Renga configuration."""
    try:
        with open(config_path(), 'r') as configfile:
            return yaml.load(configfile) or {}
    except FileNotFoundError:
        return {}


def write_config(config):
    """Write Renga configuration."""
    with open(config_path(), 'w+') as configfile:
        yaml.dump(config, configfile, default_flow_style=False)


def with_config(f):
    """Add config to function."""
    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        result = ctx.invoke(f, ctx.obj['config'], *args, **kwargs)
        write_config(ctx.obj['config'])
    return update_wrapper(new_func, f)
