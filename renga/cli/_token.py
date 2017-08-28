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
"""Perform token operations."""

from contextlib import contextmanager

import click
import requests


def offline_token_using_password(token_endpoint, client_id, username,
                                 password):
    """Get offine token using password."""
    response = requests.post(
        token_endpoint,
        data={
            'grant_type': 'password',
            'scope': ['offline_access', 'openid'],
            'client_id': client_id,
            'username': username,
            'password': password,
        })
    return response.json()


def exchange_token(refresh_token, token_endpoint, client_id):
    """Exchange token for access token."""
    response = requests.post(
        token_endpoint,
        data={
            'grant_type': 'refresh_token',
            'client_id': client_id,
            'refresh_token': refresh_token,
        })
    return response.json()


@contextmanager
def with_access_token(config, endpoint):
    """Yield access token for endpoint in the config."""
    token = config['endpoints'][endpoint]['token']
    url = config['endpoints'][endpoint]['url']
    client_id = config['endpoints'][endpoint]['client_id']
    data = exchange_token(token, url, client_id)

    if 'error' in data:
        raise click.ClickException(
            '{error_description} ({error})'.format(**data))

    yield data['access_token']
