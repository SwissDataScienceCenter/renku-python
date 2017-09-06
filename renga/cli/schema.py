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
"""Manage schemas."""

import json

import click

from renga.cli._options import option_endpoint
from renga.client import RengaClient

from ._config import with_config
from ._token import with_access_token


@click.command()
@option_endpoint
@with_config
def schema(config, endpoint):
    """Display combined schema for all services."""
    click.echo(json.dumps(RengaClient(endpoint).swagger()))
