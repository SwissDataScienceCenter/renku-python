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
"""Authorization service."""

from oauthlib.oauth2 import LegacyApplicationClient as LAC
from requests_oauthlib import OAuth2Session


class LegacyApplicationClient(LAC):
    """A public client using the password and username directly.

    Provides defaults for simple usage with CLI.
    """

    def __init__(self, client_id, **kwargs):
        """Define default scopes."""
        kwargs.setdefault('scope', ['offline_access', 'openid'])
        super(LegacyApplicationClient, self).__init__(client_id, **kwargs)


class AuthorizationMixin(OAuth2Session):
    """Client for managing authorization."""

    TOKEN_URL = '{0}/auth/realms/Renku/protocol/openid-connect/token'

    def __init__(self, **kwargs):
        """Define default client."""
        kwargs.setdefault(
            'client',
            LegacyApplicationClient(
                kwargs.get('client_id'), token=kwargs.get('token')
            )
        )

        def token_updater(token):
            """Dummy token updater."""
            pass

        kwargs.setdefault('token_updater', token_updater)
        kwargs.setdefault(
            'auto_refresh_url', self.TOKEN_URL.format(self.endpoint)
        )

        super(AuthorizationMixin, self).__init__(**kwargs)

    def fetch_token(self, token_url=None, **kwargs):
        """Set default ``token_url``."""
        kwargs.setdefault('auth', lambda x: x)
        token_url = token_url or self.TOKEN_URL.format(self.endpoint)
        return super(AuthorizationMixin, self).fetch_token(token_url, **kwargs)

    def refresh_token(self, token_url=None, **kwargs):
        """Set default ``token_url``."""
        token_url = token_url or self.TOKEN_URL.format(self.endpoint)
        kwargs.setdefault('client_id', self.client_id)
        return super(AuthorizationMixin,
                     self).refresh_token(token_url, **kwargs)
