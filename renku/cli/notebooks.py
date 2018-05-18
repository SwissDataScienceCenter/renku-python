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
"""Manage notebookss."""

import os
from binascii import hexlify

import click

from renku.cli._options import option_endpoint

from ..models._tabulate import tabulate
from ..models.deployer import _dict_from_labels
from ._client import from_config
from ._config import with_config
from .deployments import create


@click.group()
def notebooks():
    """Manage notebooks."""


@notebooks.command()
@click.option(
    '-u',
    '--url',
    help='URL of an OpenID Connect server.',
    envvar='RENKU_OIC_URL',
)
@click.option(
    '-i',
    '--client-id',
    help='Client identifer for OAuth 2.0.',
    envvar='RENKU_OIC_CLIENT_ID',
)
def configure(url, client_id):
    """Configure the Jupyter server extension."""
    from notebook.services.config import ConfigManager

    cm = ConfigManager()
    cm.update('renku.notebook.oic', {
        'client_id': client_id,
        'url': url,
    })


@notebooks.command()
@option_endpoint
@click.option('--all', is_flag=True)
@with_config
def list(config, all, endpoint):
    """Show running notebooks."""
    project_config = config.get('project', {})
    project_vertex_id = project_config.get('endpoints',
                                           {}).get(endpoint,
                                                   {}).get('vertex_id')

    client = from_config(config, endpoint=endpoint)

    def is_notebook(context):
        return 'renku.notebook.token' in context.labels and (
            project_vertex_id ==
            context.labels.get('renku.project.vertex_id', 0) or all
        )

    click.echo(
        tabulate((
            execution for context in client.contexts if is_notebook(context)
            for execution in context.executions
        ),
                 headers=('engine', 'url'),
                 showindex='always')
    )


@notebooks.command()
@click.option(
    '--context', default=None, help='Use an existing notebook context.'
)
@click.option('--engine', default='docker')
@click.option(
    '--image',
    default='renku/minimal-notebook:latest',
    help='Notebook image to use.'
)
@click.option('--input', multiple=True, help='Named input context slots.')
@click.option('--output', multiple=True, help='Named output context slots.')
@option_endpoint
@with_config
@click.pass_context
def launch(ctx, config, context, engine, image, input, output, endpoint):
    """Launch a new notebook."""
    cfg = config.get('project', config)['endpoints'][endpoint]
    if ':' not in image:
        image += ':latest'

    # Store defined inputs and outputs as a mapping.
    inputs = {}
    input = [i for i in input]
    outputs = {}
    output = [o for o in output]

    for name, value in _dict_from_labels(input).items():
        inputs[name] = value

    for name, value in _dict_from_labels(output).items():
        outputs[name] = value
    # end block of inputs and outputs handling.

    if context is not None:
        client = from_config(config, endpoint=endpoint)
        context = client.contexts[context]

    if not context:
        notebook_token = hexlify(os.urandom(24)).decode('ascii')

        context = ctx.invoke(
            create,
            command="start-notebook.sh "
            "--ContentsManager.untitled_notebook=notebook "
            "--NotebookApp.ip='*' "
            "--NotebookApp.token={0} "
            "--NotebookApp.contents_manager_class="
            "renku.notebook.RenkuStorageManager".format(notebook_token),
            ports=['8888'],
            image=image,
            labels=['renku.notebook.token={0}'.format(notebook_token)],
            input=input,
            output=output,
            endpoint=endpoint
        )
        cfg = ctx.obj['config'].get('project', config)['endpoints'][endpoint]
        proj_notebooks = cfg.get('notebooks', {})
        proj_notebooks[image] = context.id
        cfg['notebooks'] = proj_notebooks

    if 'notebook' not in context.inputs._names:
        click.echo(
            'Option "--input notebook[=ID]" is missing. '
            'The new notebook will not be tracked.'
        )

    execution = context.run(
        engine=engine,
        inputs=inputs,
        outputs=outputs,
    )
    click.echo(execution.url)
