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
"""CLI for the Renga platform."""

import errno
import os
from functools import update_wrapper

import click
import requests
import yaml
from click_plugins import with_plugins
from openid_connect import OpenIDClient
from pkg_resources import iter_entry_points

from ._config import config_path, read_config, with_config, write_config


def print_version(ctx, param, value):
    """Print version number."""
    from renga.version import __version__

    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)
    ctx.exit()


@with_plugins(iter_entry_points('renga.cli'))
@click.group(context_settings={
    'auto_envvar_prefix': 'RENGA',
})
@click.option(
    '--version',
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help=print_version.__doc__)
@click.pass_context
def cli(ctx):
    """Base cli."""
    ctx.obj = {
        'config': read_config(),
    }


@cli.command()
@click.argument('endpoint', nargs=1)
@click.option('--url', default='{endpoint}/auth/realm/TEST/')
@click.option('--client-id', default='demo-client')
@click.option('--username', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
@with_config
def login(config, endpoint, url, client_id, username, password):
    """Initialize tokens for access to the platform."""
    url = url.format(endpoint=endpoint, client_id=client_id)
    client = OpenIDClient(url, client_id, None)
    response = requests.post(
        client.token_endpoint,
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


@cli.command()
@click.option('--autosync', is_flag=True)
@click.argument('project_name', nargs=1)
def init(project_name, autosync):
    """Initialize a project."""
    if not autosync:
        raise click.UsageError('You must specify the --autosync option.')

    # 1. create the directory
    try:
        os.mkdir(project_name)
    except FileExistsError:
        raise click.UsageError(
            'Directory {0} already exists'.format(project_name))
