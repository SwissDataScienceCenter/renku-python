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
"""Login to the Renga platform."""

import click
import requests

from renga import APIClient
from renga.api.authorization import LegacyApplicationClient

from ._client import from_config
from ._config import config_path, with_config
from ._options import argument_endpoint, default_endpoint


@click.command()
@click.argument('endpoint', required=False, callback=default_endpoint)
@click.option(
    '--url',
    default='{endpoint}/auth/realms/Renga/protocol/openid-connect/token')
@click.option('--client-id', default='demo-client')
@click.option('--username', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
@click.option('--default', is_flag=True)
@with_config
def login(config, endpoint, url, client_id, username, password, default):
    """Initialize tokens for access to the platform."""
    url = url.format(endpoint=endpoint, client_id=client_id)
    scope = ['offline_access', 'openid']

    config.setdefault('endpoints', {})
    config['endpoints'].setdefault(endpoint, {})
    config['endpoints'][endpoint].setdefault('token', {})
    config['endpoints'][endpoint]['client_id'] = client_id
    config['endpoints'][endpoint]['url'] = url

    client = from_config(config, endpoint=endpoint)
    token = client.api.fetch_token(
        url,
        username=username,
        password=password,
        client_id=client_id,
        scope=scope)

    config['endpoints'][endpoint]['token'] = dict(token)

    if len(config['endpoints']) == 1 or default:
        config.setdefault('core', {})
        config['core']['default'] = endpoint

    click.echo('Access token has been stored in: {0}'.format(config_path()))


@click.group(invoke_without_command=True)
@with_config
@click.pass_context
def tokens(ctx, config):
    """Print access tokens."""
    if ctx.invoked_subcommand is None:
        for url, data in config.get('endpoints').items():
            click.echo('{url}: {token}'.format(
                url=url, token=data['token']['refresh_token']))


@tokens.command()
@argument_endpoint
@with_config
@click.pass_context
def access(ctx, config, endpoint):
    """Try to get access token."""
    client = from_config(config, endpoint=endpoint)
    click.echo(client.api.refresh_token()['access_token'])
