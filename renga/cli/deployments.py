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

import click
import json

from ._config import with_config
from ._token import with_access_token
from renga.clients.deployer import DeployerClient
from renga.cli._options import option_endpoint


@click.group(invoke_without_command=True)
@with_config
@option_endpoint
@click.pass_context
def contexts(ctx, config, endpoint):
    """Manage execution contexts."""
    if ctx.invoked_subcommand is None:
        deployer_client = DeployerClient(endpoint)

        with with_access_token(config, endpoint) as token:
            for context in deployer_client.list_contexts(token)['contexts']:
                click.echo(json.dumps(context))


@click.group()
@with_config
@click.pass_context
def executions(ctx, config):
    """Manage executions."""
    pass


@executions.command()
@click.argument('context_id')
@with_config
@option_endpoint
def show(config, context_id, endpoint):
    """Show the executions of a context."""
    deployer_client = DeployerClient(endpoint)

    with with_access_token(config, endpoint) as token:
        for execution in deployer_client.list_executions(context_id,
                                                         token)['executions']:
            click.echo(json.dumps(execution))


@executions.command()
@click.argument('context_id')
@click.argument('execution_id')
@with_config
@option_endpoint
def ports(config, context_id, execution_id, endpoint):
    """Show the port and host mapping of an execution."""
    deployer_client = DeployerClient(endpoint)

    with with_access_token(config, endpoint) as token:
        click.echo(deployer_client.get_ports(context_id, execution_id, token))

