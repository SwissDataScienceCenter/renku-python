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
"""Login to the Renga platform."""

import click
import requests

from ._config import config_path, with_config


@click.command()
@click.argument('endpoint', nargs=1)
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
    response = requests.post(
        url,
        data={
            'grant_type': 'password',
            'scope': ['offline_access', 'openid'],
            'client_id': client_id,
            'username': username,
            'password': password,
        })
    data = response.json()
    config.setdefault('endpoints', {})
    config['endpoints'].setdefault(endpoint, {})
    config['endpoints'][endpoint]['url'] = url
    config['endpoints'][endpoint]['token'] = data['refresh_token']

    if len(config['endpoints']) == 1 and default:
        config.setdefault('core', {})
        config['core']['default'] = endpoint

    click.echo('Access token has been stored in: {0}'.format(config_path()))


@click.command()
@with_config
def tokens(config):
    """Print access tokens."""
    for url, data in config.get('endpoints').items():
        click.echo('{url}: {token}'.format(url=url, token=data['token']))
