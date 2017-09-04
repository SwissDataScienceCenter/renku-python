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
"""Interact with the deployment service."""

import json

import click

from renga.cli._options import option_endpoint
from renga.client import RengaClient

from ._config import with_config
from ._token import with_access_token


@click.group(invoke_without_command=True)
@option_endpoint
@with_config
@click.pass_context
def contexts(ctx, config, endpoint):
    """Manage execution contexts."""
    if ctx.invoked_subcommand is None:
        with with_access_token(config, endpoint) as access_token:
            deployer_client = RengaClient(endpoint, access_token).deployer
            for context in deployer_client.list_contexts():
                click.echo(json.dumps(context))


@click.group()
def executions():
    """Manage executions."""


@executions.command()
@click.argument('context_id')
@option_endpoint
@with_config
def show(config, context_id, endpoint):
    """Show the executions of a context."""
    with with_access_token(config, endpoint) as access_token:
        deployer_client = RengaClient(endpoint, access_token).deployer
        for execution in deployer_client.list_executions(context_id):
            click.echo(json.dumps(execution))


@executions.command()
@click.argument('context_id')
@click.argument('execution_id')
@option_endpoint
@with_config
def ports(config, context_id, execution_id, endpoint):
    """Show the port and host mapping of an execution."""
    with with_access_token(config, endpoint) as access_token:
        deployer_client = RengaClient(endpoint, access_token).deployer
        click.echo(deployer_client.get_ports(context_id, execution_id))
