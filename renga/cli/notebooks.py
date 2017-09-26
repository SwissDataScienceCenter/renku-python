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

import datetime
import json
import os
from binascii import hexlify

import click

from renga import errors
from renga.cli._options import option_endpoint

from ..models._tabulate import tabulate
from ..models.deployer import _dict_from_labels
from ._client import from_config
from ._config import with_config
from .deployments import create


@click.group()
@with_config
@click.pass_context
def notebooks(ctx, config):
    """Manage notebooks."""


@notebooks.command()
@option_endpoint
@click.option('--all', is_flag=True)
@with_config
def list(config, all, endpoint):
    """Show running notebooks."""
    project_config = config.get('project', {})
    project_vertex_id = project_config.get('endpoints', {}).get(
        endpoint, {}).get('vertex_id')

    client = from_config(config, endpoint=endpoint)

    def is_notebook(context):
        return 'renga.notebook.token' in context.labels and (
            project_vertex_id == context.labels.get('renga.project.vertex_id',
                                                    0) or all)

    click.echo(
        tabulate(
            (execution for context in client.contexts
             if is_notebook(context) for execution in context.executions),
            headers=('engine', 'url'),
            showindex='always'))


@notebooks.command()
@click.option('--engine', default='docker')
@click.option(
    '--image',
    default='jupyter/minimal-notebook:latest',
    help='Notebook image to use.')
@option_endpoint
@with_config
@click.pass_context
def launch(ctx, config, engine, image, endpoint):
    """Launch a new notebook."""
    cfg = config.get('project', config)['endpoints'][endpoint]
    if ':' not in image:
        image += ':latest'

    if image in cfg.get('notebooks', {}):
        client = from_config(config, endpoint=endpoint)
        context_id = cfg['notebooks'][image]

        try:
            context = client.contexts[context_id]
        except errors.APIError:
            context = None
    else:
        context = None

    if not context:
        notebook_token = hexlify(os.urandom(24)).decode('ascii')

        context = ctx.invoke(
            create,
            command="start-notebook.sh --NotebookApp.token={0}".format(
                notebook_token),
            ports=['8888'],
            image=image,
            labels=['renga.notebook.token={0}'.format(notebook_token)],
            endpoint=endpoint)
        cfg = ctx.obj['config'].get('project', config)['endpoints'][endpoint]
        proj_notebooks = cfg.get('notebooks', {})
        proj_notebooks[image] = context.id
        cfg['notebooks'] = proj_notebooks

    execution = context.run(engine=engine)
    click.echo(execution.url)
