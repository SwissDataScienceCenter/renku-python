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
"""Authorization service."""

import requests

from renga.client._datastructures import Endpoint, EndpointMixin


class AuthorizationClient(EndpointMixin):
    """Client for managing authorization."""

    authorization_url = Endpoint('/auth/realms/Renga/protocol/'
                                 'openid-connect/auth')
    token_url = Endpoint('/auth/realms/Renga/protocol/openid-connect/token')

    @property
    def is_service_authorized(self):
        """Return ``True`` if service authorization exists."""
        return hasattr(self, '_service_authorization') \
            and 'access_token' in self._service_authorization

    def authorize_service(self,
                          audience=None,
                          client_id=None,
                          client_secret=None):
        """Retrieve a service access token."""
        response = requests.post(
            self.token_url,
            data={
                'audience': audience,
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials',
            })
        self._service_authorization = response.json()
        return 'access_token' in self._service_authorization

    @property
    def service_headers(self):
        """Return service headers."""
        return {
            'Authorization':
            'Bearer {0}'.format(self._service_authorization['access_token'])
        }
