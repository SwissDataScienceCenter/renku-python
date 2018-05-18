# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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

from ..models._tabulate import tabulate
from ._client import from_config
from ._config import with_config
from ._group import OptionalGroup
from ._options import option_endpoint


@click.group(name='io')
def storage():
    """Manage storage."""


@storage.command()
@option_endpoint
@with_config
def backends(config, endpoint):
    """List all available storage backends."""
    client = from_config(config, endpoint=endpoint)
    for backend in client.buckets.backends:
        click.echo(backend)


@storage.group(cls=OptionalGroup)
@click.argument('bucket_id', required=False)
@click.pass_context
def buckets(ctx, bucket_id):
    """Bucket manipulation."""
    ctx.obj['bucket_id'] = int(bucket_id) if bucket_id else None


@buckets.command()
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
            'default_bucket', bucket.id
        )

    click.echo(bucket.id)


@buckets.command()
@click.option(
    '-a', '--all', 'all_buckets', is_flag=True, help='Show all buckets.'
)
@click.option('--sort-by', type=click.Choice(['id', 'name']), default='id')
@option_endpoint
@with_config
def list(config, endpoint, all_buckets, sort_by):
    """List buckets."""
    client = from_config(config, endpoint=endpoint)
    buckets = client.buckets
    headers = buckets.Meta.headers
    filter_ids = None

    if 'project' in config:
        filter_ids = set(
            config['project']['endpoints'][endpoint].get('buckets', {}).keys()
        )

    # filter out non-project buckets if needed
    if filter_ids and not all_buckets:
        buckets = (bucket for bucket in buckets if bucket.id in filter_ids)

    buckets = sorted(buckets, key=lambda b: getattr(b, sort_by))
    click.echo(tabulate(buckets, headers=headers))


@buckets.command()
@option_endpoint
@with_config
@click.pass_context
def files(ctx, config, endpoint):
    """List files in a bucket."""
    bucket_id = ctx.obj.get('bucket_id')

    if bucket_id is None:
        raise click.MissingParameter(
            'bucket has to be defined', ctx=ctx, param_hint='bucket'
        )

    client = from_config(config, endpoint=endpoint)
    bucket = client.buckets[bucket_id]

    if bucket.files:
        click.echo(tabulate(bucket.files, headers=bucket.files.Meta.headers))


@buckets.command()
@click.argument('input', default='-', type=click.File('wb'))
@click.option('--name', default=None, type=click.STRING)
@option_endpoint
@with_config
@click.pass_context
def upload(ctx, config, input, name, endpoint):
    """Create file from an input in the bucket."""
    bucket_id = ctx.obj.get('bucket_id')

    if bucket_id is None:
        raise click.MissingParameter(
            'bucket has to be defined', ctx=ctx, param_hint='bucket_id'
        )

    client = from_config(config, endpoint=endpoint)
    bucket = client.buckets[bucket_id]
    try:
        name = name or input.name
    except AttributeError:
        raise click.MissingParameter(
            'name has to be define when using STDIN',
            ctx=ctx,
            param_hint='name'
        )

    with bucket.files.open(name, 'w') as fp:
        fp.write(input.read())

    click.echo(fp.id)


@buckets.command()
@click.argument('file_id', required=True, type=int)
@click.argument('output', default='-', type=click.File('wb'))
@option_endpoint
@with_config
@click.pass_context
def download(ctx, config, file_id, output, endpoint):
    """Download a file from a bucket."""
    bucket_id = ctx.obj.get('bucket_id')

    if bucket_id is None:
        raise click.MissingParameter(
            'bucket has to be defined', ctx=ctx, param_hint='bucket_id'
        )

    client = from_config(config, endpoint=endpoint)
    file_ = client.buckets[bucket_id].files[file_id]

    with file_.open('r') as fp:
        output.write(fp.read())
