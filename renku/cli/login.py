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

import click
import http.server
import logging
import socketserver
import threading
import webbrowser

from ._client import from_config
from ._config import config_path, with_config
from ._options import argument_endpoint, default_endpoint

logger = logging.getLogger('renku.cli.login')


class AuthServer(socketserver.TCPServer):
    """A TCPServer with some extra local variables."""

    def __init__(self, ctx, client, config, endpoint, url, client_id, default,
                 *args, **kwargs):
        """Initialize the server with extra parameters."""
        self.auth_ctx = ctx
        self.auth_client = client
        self.auth_config = config
        self.auth_endpoint = endpoint
        self.auth_client_id = client_id
        self.auth_url = url
        self.auth_default = default
        super().__init__(*args, **kwargs)


class AuthHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """A very simple http server to handle the redirect with the code."""

    def do_GET(self):
        """Endpoint for the redirect with the code."""
        scope = ['offline_access', 'openid']
        token = self.server.auth_client.api.fetch_token(
            '{}/protocol/openid-connect/token'.format(self.server.auth_url),
            authorization_response=self.path,
            client_id=self.server.auth_client_id,
            scope=scope,
        )
        config = self.server.auth_config

        config['endpoints'][self.server.auth_endpoint]['token'] = dict(token)

        if len(config['endpoints']) == 1 or self.server.auth_default:
            config.setdefault('core', {})
            config['core']['default'] = self.server.auth_endpoint

        click.echo(
            'Access token has been stored in: {0}'.format(
                config_path(self.server.auth_ctx.obj.get('config_path'))
            )
        )
        text = b"<html><body>You can close this.</body></html>"
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-Length", str(len(text)))
        self.end_headers()
        self.wfile.write(text)

        assassin = threading.Thread(target=self.server.shutdown)
        assassin.daemon = True
        assassin.start()

    def log_message(self, format, *args):
        """Logging with httpd format."""
        logger.debug("%s - - [%s] %s\n" % (
            self.address_string(),
            self.log_date_time_string(), format % args)
        )


@click.command()
@click.argument('endpoint', required=False, callback=default_endpoint)
@click.option(
    '--url',
    default='{endpoint}/auth/realms/Renku'
)
@click.option('--client-id', default='demo-client')
@click.option('--default', is_flag=True)
@with_config
@click.pass_context
def login(ctx, config, endpoint, url, client_id, default):
    """Initialize tokens for access to the platform."""
    url = url.format(endpoint=endpoint, client_id=client_id)

    config.setdefault('endpoints', {})
    config['endpoints'].setdefault(endpoint, {})
    config['endpoints'][endpoint].setdefault('token', {})
    config['endpoints'][endpoint]['client_id'] = client_id
    config['endpoints'][endpoint]['url'] = url

    client = from_config(config, endpoint=endpoint)

    authorization_url, _ = client.api.authorization_url(
        '{url}/protocol/openid-connect/auth'.format(url=url))

    webbrowser.open_new_tab(authorization_url)

    with AuthServer(ctx, client, config, endpoint, url, client_id, default,
                    ("", 5000), AuthHTTPRequestHandler) as httpd:
        httpd.serve_forever()


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
