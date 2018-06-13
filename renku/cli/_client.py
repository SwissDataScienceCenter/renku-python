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
"""Client utilities."""

import uuid

import click
import yaml

from renku.api import LocalClient
from renku.client import RenkuClient

from ._config import get_project_config_path, read_config
from ._options import default_endpoint_from_config


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


def from_config(config=None, endpoint=None):
    """Create a new client for endpoint in the config.

    Use ``renku`` command-line interface to manage multiple
    configurations.
    """
    if config is None:
        config = read_config()
        project_config_path = get_project_config_path()
        if project_config_path:
            config['project'] = read_config(project_config_path)

    endpoint = endpoint or default_endpoint_from_config(config)
    token = config['endpoints'][endpoint]['token']
    url = config['endpoints'][endpoint]['url']
    client_id = config['endpoints'][endpoint]['client_id']

    client = RenkuClient(
        endpoint, client_id=client_id, token=token, auto_refresh_url=url
    )

    if 'project' in config:
        client.api.headers['Renku-Projects-Project'] = config['project'][
            'endpoints'
        ][endpoint]['vertex_id']

    return client


pass_local_client = click.make_pass_decorator(LocalClient, ensure=True)
