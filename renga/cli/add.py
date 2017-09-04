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
"""Manage files."""

import datetime
import os

import click

from renga.client import RengaClient
from renga.client.storage import CreateFile

from ._config import with_config
from ._options import option_endpoint
from ._token import with_access_token


@click.command()
@click.argument('pathspec')
@option_endpoint
@with_config
def add(config, pathspec, endpoint):
    """Add a resource to the project."""
    config['project'].setdefault('resources', {})
    resources = config['project']['resources']

    # TODO check that the pathspec is relative to project directory

    if pathspec in resources:
        raise click.UsageError('Resource already exists.')

    resource = {
        'added': datetime.datetime.utcnow().isoformat(),
    }

    autosync = config['project']['core']['autosync']
    if autosync:
        bucket_id = config['project']['endpoints'][endpoint]['default_bucket']
        resource.setdefault('endpoints', {})

        with with_access_token(config, endpoint) as access_token:
            client = RengaClient(
                endpoint=endpoint, access_token=access_token)
            file_ = client.storage.create_file(CreateFile(
                bucket_id=bucket_id,
                file_name=pathspec,
            ))

            resource['endpoints'][endpoint] = {
                'vertex_id': file_['id'],
                'access_token': file_['access_token'],
            }

    config['project']['resources'][pathspec] = resource
