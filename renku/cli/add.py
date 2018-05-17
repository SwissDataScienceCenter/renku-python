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
"""Add files to a project.

Adding data to a project
~~~~~~~~~~~~~~~~~~~~~~~~

In a newly-initialized project directory, nothing is tracked yet. You can
start tracking files by adding them to Renku with e.g.:

.. code-block:: console

    $ renku add input.csv

If you want to add a file to a specific bucket, you can do so by using the
``--bucket-id`` option.
"""

import datetime

import click

from ._client import from_config
from ._config import with_config
from ._options import option_endpoint


@click.command()
@click.argument('path', type=click.File('rb'))
@option_endpoint
@click.option('--bucket-id', required=False, default=None, type=int)
@with_config
def add(config, path, endpoint, bucket_id):
    """Add a file to the project."""
    config['project'].setdefault('resources', {})
    resources = config['project']['resources']

    # TODO check that the path is relative to project directory

    if path.name in resources:
        raise click.UsageError('Resource already exists.')

    resource = {
        'added': datetime.datetime.utcnow().isoformat(),
    }

    bucket_id = bucket_id or \
        config['project']['endpoints'][endpoint]['default_bucket']
    resource.setdefault('endpoints', {})

    client = from_config(config, endpoint=endpoint)
    bucket = client.buckets[bucket_id]

    with bucket.files.open(path.name, 'w') as fp:
        fp.write(path)

    resource['endpoints'][endpoint] = {
        'vertex_id': fp.id,
    }
    click.echo(fp.id)

    config['project']['resources'][path.name] = resource
