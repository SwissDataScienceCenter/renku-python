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
"""Handle storage API."""

import click
import requests

from ._client import from_config
from ._config import config_path, with_config
from ._options import option_endpoint


@click.group(name='io', invoke_without_command=True)
@with_config
@click.pass_context
def storage(ctx, config):
    """Manage storage."""
    if ctx.invoked_subcommand is None:
        click.echo('Try --help')


@storage.command()
@option_endpoint
@with_config
def backends(config, endpoint):
    """List all available storage backends."""
    client = from_config(config, endpoint=endpoint)
    for backend in client.buckets.backends:
        click.echo(backend)


@storage.group()
def bucket():
    """Bucket manipulation."""


@bucket.command()
@click.argument('name')
@click.option('-b', '--backend', default='local')
@option_endpoint
@with_config
def create(config, name, backend, endpoint):
    """Create new bucket."""
    client = from_config(config, endpoint=endpoint)
    bucket = client.buckets.create(name=name, backend=backend)

    if 'project' in config:
        config['project']['endpoints'].setdefault(endpoint, {})
        config['project']['endpoints'][endpoint].setdefault('buckets', {})
        config['project']['endpoints'][endpoint]['buckets'][bucket.id] = name

        # Set default bucket
        config['project']['endpoints'][endpoint].setdefault(
            'default_bucket', bucket.id)

    click.echo(bucket.id)


@bucket.command()
@option_endpoint
@with_config
def list(config, endpoint):
    """List buckets."""
    buckets = config['project']['endpoints'][endpoint].get('buckets', {})

    if buckets is None:
        raise click.ClickException('No registered buckets')

    for bucket_id, name in buckets.items():
        click.echo('{0}\t{1}'.format(name, bucket_id))
