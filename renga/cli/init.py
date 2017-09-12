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
"""Initialize a Renga project."""

import datetime
import os

import click

from renga.client import RengaClient

from ._client import from_config
from ._config import create_project_config_path, get_project_config_path, \
    read_config, with_config, write_config
from ._options import option_endpoint


def validate_name(ctx, param, value):
    """Validate a project name."""
    if not value:
        value = os.path.basename(ctx.params['directory'].rstrip(os.path.sep))
    return value


@click.command()
@click.argument(
    'directory',
    default='.',
    type=click.Path(
        exists=True, writable=True, file_okay=False, resolve_path=True))
@click.option('--autosync', is_flag=True)
@click.option('--name', callback=validate_name)
@click.option('--force', is_flag=True)
@option_endpoint
@with_config
def init(config, directory, autosync, name, force, endpoint):
    """Initialize a project."""
    if not autosync:
        raise click.UsageError('You must specify the --autosync option.')

    # 1. create the directory
    try:
        project_config_path = create_project_config_path(directory)
    except FileExistsError as e:
        if not force:
            raise click.UsageError(
                'Directory {0} is already initialized'.format(directory))
        project_config_path = e.filename

    project_config = read_config(project_config_path)
    project_config.setdefault('core', {})
    project_config['core']['autosync'] = autosync
    project_config['core']['name'] = name
    project_config['core'].setdefault('generated',
                                      datetime.datetime.utcnow().isoformat())

    if autosync:
        client = from_config(config, endpoint=endpoint)
        project = client.projects.create(name=name)
        project_config.setdefault('endpoints', {})
        project_config['endpoints'].setdefault(endpoint, {})
        project_config['endpoints'][endpoint][
            'vertex_id'] = project.id

    write_config(project_config, path=project_config_path)
    click.echo('Initialized empty project in {0}'.format(project_config_path))
