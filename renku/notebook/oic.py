# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Authenticate a notebok user using OpenID Connect."""

from __future__ import absolute_import

from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join
from openid_connect import OpenIDClient
from tornado.web import MissingArgumentError


def _jupyter_server_extension_paths():
    """Return Jupyter server extension paths."""
    return [{'module': 'renku.notebook.oic'}]


def load_jupyter_server_extension(nb_server_app):
    """Call when the extension is loaded.

    :param nb_server_app: Handle to the Notebook webserver instance.
    """
    web_app = nb_server_app.web_app
    host_pattern = '.*$'

    route_pattern = url_path_join(web_app.settings['base_url'], '/oauth')
    web_app.add_handlers(host_pattern, [(route_pattern, OAuthHandler)])


class OAuthHandler(IPythonHandler):
    """Handle OAuth redirects."""

    def get(self):
        """Authenticate user using OpenID Connect."""
        base_url = self.request.protocol + "://" + self.request.host

        # Read client configuration.
        client_config = self.config_manager.get('renku.notebook.oic') or {}
        url = client_config.get(
            'url', 'https://testing.datascience.ch/auth/realms/Renku/'
        )
        client_id = client_config.get('client_id', 'jupyter')

        # Create client for OpenID Connect service.
        client = OpenIDClient(url, client_id, None)

        try:
            code = self.get_query_argument('code')
        except MissingArgumentError:
            self.redirect(client.authorize(base_url + self.request.path, None))
            return  # in case redirect doesn't raise

        token = client.request_token(base_url + self.request.path, code)
        assert token

        # TODO use the real login mechanism
        self.redirect(base_url + '/?token={}'.format(self.token))
