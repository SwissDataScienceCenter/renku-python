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
"""Manage notebooks."""

import datetime
import os

import click
import json

from ._config import with_config
from ._token import with_access_token
from renga.cli._options import option_endpoint
from renga.clients.deployer import DeployerClient


@click.group(invoke_without_command=True)
@with_config
@click.pass_context
def notebook(ctx, config):
    """Manage notebooks."""
    if ctx.invoked_subcommand is None:
        click.echo('Listing all running notebooks:')
        ctx.invoke(show, all=True)


@notebook.command()
@with_config
@option_endpoint
@click.option('--all', is_flag=True)
def show(config, all, endpoint):
    """Show running notebooks."""
    deployer_client = DeployerClient(endpoint)

    project_config = config.get('project', {})

    project_vertex_id = project_config.get('endpoints', {}).get(
        endpoint, {}).get('vertex_id')

    contexts = []
    with with_access_token(config, endpoint) as token:
        for context in deployer_client.list_contexts(token):
            if 'jupyter' in context['spec']['image']:
                # if we are inside a project, only show project notebooks
                labels = context['spec'].get('labels', {})

                if (project_vertex_id == labels.get('renga.project.vertex_id',
                                                    0)) or all:
                    contexts.append(context)

        for context in contexts:
            for execution in deployer_client.list_executions(
                    context['identifier'], token):

                ports = deployer_client.get_ports(
                    context['identifier'], execution['identifier'], token)
                click.echo('http://{0}:{1}'.format(ports[0]['host'], ports[0][
                    'exposed']))


@notebook.command()
@with_config
@option_endpoint
@click.option('--engine', default='docker')
def launch(config, engine, endpoint):
    """Launch a new notebook."""
    deployer_client = DeployerClient(endpoint)

    project_config = config.get('project', {})

    project_vertex_id = project_config.get('endpoints', {}).get(
        endpoint, {}).get('vertex_id')

    spec = {
        'image':
        'jupyter/minimal-notebook',
        'ports': ['8888'],
        'command':
        "start-notebook.sh --NotebookApp.password='sha1:d9fb32fb1c07:520b857f8e4e6859e02d66bcf3770cf58c810079'"
    }

    if project_vertex_id:
        spec['labels'] = {'renga.project.vertex_id': project_vertex_id}

    with with_access_token(config, endpoint) as token:
        context = deployer_client.create_context(spec, token)
        execution = deployer_client.create_execution(context['identifier'],
                                                     engine, token)
        ports = deployer_client.get_ports(context['identifier'],
                                          execution['identifier'], token)
        click.echo(
            'http://{0}:{1}'.format(ports[0]['host'], ports[0]['exposed']))
