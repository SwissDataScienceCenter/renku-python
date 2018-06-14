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

"""

import uuid
import webbrowser

import click
import requests

from ._config import config_path, with_config
from ._options import default_endpoint


@click.command()
@click.argument('endpoint', required=False, callback=default_endpoint)
@click.option('--url', default='{endpoint}/auth')
@click.option('--default', is_flag=True)
@with_config
@click.pass_context
def login(ctx, config, endpoint, url, default):
    """Initialize tokens for access to the platform."""
    url = url.format(endpoint=endpoint)

    config.setdefault('endpoints', {})
    config['endpoints'].setdefault(endpoint, {})
    config['endpoints'][endpoint].setdefault('token', {})
    config['endpoints'][endpoint]['url'] = url

    uid = str(uuid.uuid4())

    webbrowser.open_new_tab(
        "{url}/login?cli_token={uid}&scope=offline_access+openid".format(
            url=url, uid=uid
        )
    )

    token = requests.get(
        "{url}/info?cli_token={uid}".format(url=url, uid=uid)
    ).json()

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
