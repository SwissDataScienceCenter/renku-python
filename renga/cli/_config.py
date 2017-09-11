# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Configuration utilities."""

import errno
import os
from functools import update_wrapper
from pathlib import Path

import click
import yaml

APP_NAME = 'Renga'
"""Application name for storing configuration."""

PROJECT_DIR = '.renga'
"""Project directory name."""


def config_path(path=None):
    """Return config path."""
    if path is None:
        path = os.environ.get('RENGA_CONFIG', click.get_app_dir(APP_NAME))
        try:
            os.makedirs(path)
        except OSError as e:  # pragma: no cover
            if e.errno != errno.EEXIST:
                raise
    return os.path.join(path, 'config.yml')


def read_config(path=None):
    """Read Renga configuration."""
    try:
        with open(config_path(path=path), 'r') as configfile:
            return yaml.load(configfile) or {}
    except FileNotFoundError:
        return {}


def write_config(config, path=None):
    """Write Renga configuration."""
    with open(config_path(path=path), 'w+') as configfile:
        yaml.dump(config, configfile, default_flow_style=False)


def with_config(f):
    """Add config to function."""
    # keep it.

    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        if ctx.obj is None:
            ctx.obj = {}

        if 'config' in ctx.obj:
            config = ctx.obj['config']
        else:
            config = ctx.obj['config'] = read_config()

        project_enabled = not ctx.obj.get('no_project', False)
        project_config_path = get_project_config_path()

        if project_enabled and project_config_path:
            project_config = read_config(project_config_path)
            config['project'] = project_config
        result = ctx.invoke(f, config, *args, **kwargs)
        project_config = config.pop('project', None)
        if project_config:
            if not project_config_path:
                raise RuntimeError('Invalid config update')
            write_config(project_config, path=project_config_path)
        write_config(config)
        return result

    return update_wrapper(new_func, f)


def print_app_config_path(ctx, param, value):
    """Print application config path."""
    if not value or ctx.resilient_parsing:
        return
    click.echo(config_path())
    ctx.exit()


def create_project_config_path(path, mode=0o777, parents=False,
                               exist_ok=False):
    """Create new project configuration folder."""
    project_path = Path(path).absolute().joinpath(PROJECT_DIR)
    project_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
    return str(project_path)


def get_project_config_path(path=None):
    """Return project configuration folder if exist."""
    project_path = Path(path or '.').absolute().joinpath(PROJECT_DIR)
    if project_path.exists() and project_path.is_dir():
        return str(project_path)


def find_project_config_path(path=None):
    """Find project config path."""
    path = Path(path) if path else pathlib.Path.cwd()
    abspath = path.absolute()

    project_path = get_project_config_path(abspath)
    if project_path:
        return project_path

    for parent in abspath.parents:
        project_path = get_project_config_path(parent)
        if project_path:
            return project_path
