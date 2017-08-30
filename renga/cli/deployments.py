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


@click.group(invoke_without_command=True)
@with_config
@click.pass_context
def contexts(ctx, config):
    """List all current contexts."""
    if ctx.invoked_subcommand is None:
        endpoint = config['core']['default']
        deployer_client = DeployerClient(endpoint)

        with with_access_token(config, endpoint) as token:
            for context in deployer_client.list_contexts(token)['contexts']:
                click.echo(json.dumps(context))
