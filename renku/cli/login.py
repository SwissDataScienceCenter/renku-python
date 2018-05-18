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
"""Logging in to the Renku platform.

There is no central Renku instance, hence a platform URL **must** be
specified. Please contact your institution administrator to obtain the URL of
a running platform and necessary credentials.

Log in to a self-hosted platform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to log in to a self-hosted platform you can specify this by adding
the platform endpoint.

.. code-block:: console

    $ renku login http://localhost/

.. note::

    The warning will be shown when an unsecure protocol is used.

Non-interactive login
~~~~~~~~~~~~~~~~~~~~~

In some environments, you might need to run the ``renku login`` command
non-interactively. Using the ``--password-stdin`` flag, you can provide a
password through ``STDIN``, which also prevents the password from ending up in
the shell's history or log-files.

The following example reads a password from a file, and passes it to the
``renku login`` command using ``STDIN``:

.. code-block:: console

    $ cat ~/my_secret.txt | renku login --username demo --password-stdin

"""

import click

from ._client import from_config
from ._config import config_path, with_config
from ._options import argument_endpoint, default_endpoint, password_prompt


@click.command()
@click.argument('endpoint', required=False, callback=default_endpoint)
@click.option(
    '--url',
    default='{endpoint}/auth/realms/Renku/protocol/openid-connect/token'
)
@click.option('--client-id', default='demo-client')
@click.option('--username', prompt=True)
@click.option('--password', callback=password_prompt)
@click.option('--password-stdin', is_flag=True)
@click.option('--default', is_flag=True)
@with_config
@click.pass_context
def login(
    ctx, config, endpoint, url, client_id, username, password, password_stdin,
    default
):
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
        scope=scope
    )

    config['endpoints'][endpoint]['token'] = dict(token)

    if len(config['endpoints']) == 1 or default:
        config.setdefault('core', {})
        config['core']['default'] = endpoint

    click.echo(
        'Access token has been stored in: {0}'.format(
            config_path(ctx.obj['config_path'])
        )
    )


@click.group(invoke_without_command=True)
@with_config
@click.pass_context
def tokens(ctx, config):
    """Print access tokens."""
    if ctx.invoked_subcommand is None:
        for url, data in config.get('endpoints').items():
            click.echo(
                '{url}: {token}'.format(
                    url=url, token=data['token']['refresh_token']
                )
            )


@tokens.command()
@argument_endpoint
@with_config
@click.pass_context
def access(ctx, config, endpoint):
    """Try to get access token."""
    client = from_config(config, endpoint=endpoint)
    click.echo(client.api.refresh_token()['access_token'])
