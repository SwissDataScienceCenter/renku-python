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
"""Interact with the deployment service."""

import click

from renga.cli._options import option_endpoint

from ._client import from_config
from ._config import with_config


@click.group(invoke_without_command=True)
@option_endpoint
@with_config
@click.pass_context
def contexts(ctx, config, endpoint):
    """Manage execution contexts."""
    if ctx.invoked_subcommand is None:
        client = from_config(config, endpoint=endpoint)
        for context in client.contexts:
            click.echo(context)


@click.group()
def executions():
    """Manage executions."""


@executions.command()
@click.argument('context_id')
@option_endpoint
@with_config
def show(config, context_id, endpoint):
    """Show the executions of a context."""
    client = from_config(config, endpoint=endpoint)
    for execution in client.contexts[context_id].executions:
        click.echo(execution)


@executions.command()
@click.argument('context_id')
@click.argument('execution_id')
@option_endpoint
@with_config
def ports(config, context_id, execution_id, endpoint):
    """Show the port and host mapping of an execution."""
    client = from_config(config, endpoint=endpoint)
    execution = client.contexts[context_id].executions[execution_id]
    click.echo(execution.ports)
