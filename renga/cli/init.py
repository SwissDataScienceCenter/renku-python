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
"""Initialize a Renga project."""

import os

import click
import datetime

from ._config import create_project_config_path, get_project_config_path, \
    read_config, write_config


@click.command()
@click.argument(
    'directory',
    default='.',
    type=click.Path(exists=True, writable=True, file_okay=False))
@click.option('--autosync', is_flag=True)
def init(directory, autosync):
    """Initialize a project."""
    if not autosync:
        raise click.UsageError('You must specify the --autosync option.')

    # 1. create the directory
    try:
        project_config_path = create_project_config_path(directory)
    except FileExistsError:
        raise click.UsageError(
            'Directory {0} is already initialized'.format(directory))

    config = read_config(project_config_path)
    config.setdefault('core', {})
    config['core']['autosync'] = autosync
    config['core'].setdefault('generated',
                              datetime.datetime.utcnow().isoformat())

    write_config(config, path=project_config_path)
