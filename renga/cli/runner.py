# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Simplify running of CI scripts."""

import os
import sys
import tempfile
import uuid
from subprocess import call

import click
import yaml

from renga._compat import Path
from renga.notebook import generate_launch_args, generate_notebook_token

from ._graph import Graph
from ._repo import pass_repo


@click.group()
def runner():
    """Simplify running of CI scripts."""


@runner.command()
def template():
    """Generate template for CI."""
    raise NotImplemented()


@runner.command()
@click.option(
    '--run/--no-run', is_flag=True, envvar='RENGA_RUNNER_RERUN',
    help='Run or only load the CWL and the job description.')
@click.option('--job', envvar='RENGA_RUNNER_JOB',
              help='Job description in YAML.')
@pass_repo
def rerun(repo, run, job):
    """Re-run existing workflow or tool using CWL runner."""
    graph = Graph(repo)
    cwl = graph.find_latest_cwl()

    if not cwl:
        raise click.ClickException('No tool was found.')

    try:
        args = ['cwl-runner', cwl]
        if job:
            job_file = tempfile.NamedTemporaryFile(
                suffix='.yml', dir=os.getcwd(), delete=False
            )
            args.append(job_file.name)

            with job_file as fp:
                yaml.dump(yaml.load(job), stream=fp, encoding='utf-8')

        if run:
            call(args, cwd=os.getcwd())
    finally:
        if job:
            os.unlink(job_file.name)


@runner.command()
@click.option('--project-path', envvar='CI_PROJECT_PATH',
              default=lambda: os.path.basename(os.getcwd()))
@click.option('--name', envvar='CI_BUILD_REF_SLUG',
              default=lambda: uuid.uuid4().hex)
@click.option('--network', envvar='RENGA_RUNNER_NETWORK',
              default='bridge')
@click.option('--image', envvar='RENGA_RUNNER_IMAGE',
              default='jupyter/minimal-notebook:latest')
@pass_repo
def notebook(repo, project_path, name, network, image):
    """Launch notebook in a container."""
    token = generate_notebook_token()
    base_url = '/{project_path}/{name}'.format(
        project_path=project_path, name=name)

    args = [
        'docker', 'run',
        '--network', network,
        '--name', name,
        '--rm',
        '-v', '{0}:/home/jovyan/work'.format(Path('.').absolute()),
        '--label', 'renga.notebook.token={0}'.format(token),
        '--label', 'traefik.enable=true',
        '--label', 'traefik.frontend.rule=PathPrefix:{0}'.format(base_url),
        image,
    ] + generate_launch_args(token=token, base_url=base_url)

    call(args)
