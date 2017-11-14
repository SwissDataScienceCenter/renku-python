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
"""Create an empty Renga project or reinitialize an existing one.

Starting a Renga project
~~~~~~~~~~~~~~~~~~~~~~~~

If you have an existing directory which you want to turn into a Renga project,
you can type:

.. code-block:: console

    $ cd ~/my_project
    $ renga init

or:

.. code-block:: console

    $ renga init ~/my_project

This creates a new subdirectory named ``.renga`` that contains all the
necessary files for managing the project configuration.

Storing related data
~~~~~~~~~~~~~~~~~~~~

Each newly created project can get an automatically created storage space
(bucket) for its related data. This feature can be controlled with the
``--bucket/--no-bucket`` flag.
"""

import datetime
import os

import click

from renga.client import RengaClient

from ._client import from_config
from ._config import create_project_config_path, get_project_config_path, \
    read_config, with_config, write_config
from ._options import option_endpoint
from .io import create


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
@click.option('--autosync', is_flag=True, help='DEPRECATED')
@click.option('--name', callback=validate_name)
@click.option('--force', is_flag=True)
@option_endpoint
@click.option(
    '--bucket/--no-bucket',
    default=False,
    help='Initialize with/without a new bucket')
@with_config
@click.pass_context
def init(ctx, config, directory, autosync, name, force, endpoint, bucket):
    """Initialize a project."""
    # 1. create the directory
    try:
        project_config_path = create_project_config_path(
            directory, exist_ok=force)
    except OSError as e:
        raise click.UsageError(str(e))

    project_config = read_config(project_config_path)
    project_config.setdefault('core', {})
    project_config['core']['name'] = name
    project_config['core'].setdefault('generated',
                                      datetime.datetime.utcnow().isoformat())

    if endpoint.option is not None:
        project_config['core']['default'] = endpoint

    client = from_config(config, endpoint=endpoint)
    project = client.projects.create(name=name)
    project_config.setdefault('endpoints', {})
    project_config['endpoints'].setdefault(endpoint, {})
    project_config['endpoints'][endpoint]['vertex_id'] = project.id

    write_config(project_config, path=project_config_path)

    if bucket:
        config['project'] = project_config
        ctx.invoke(
            create.callback,
            config=config,
            name=name,
            endpoint=endpoint,
            backend='local')
        del config['project']

    click.echo('Initialized empty project in {0}'.format(project_config_path))
