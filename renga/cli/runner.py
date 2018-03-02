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
import tempfile
import uuid
from subprocess import call

import click
import pkg_resources
import yaml

from renga.notebook import generate_launch_args, generate_notebook_token

from ._client import pass_local_client

_GITLAB_CI = '.gitlab-ci.yml'


@click.group()
def runner():
    """Simplify running of CI scripts."""


@runner.command()
@pass_local_client
def template(repo):
    """Generate template for CI."""
    with open(repo.path / _GITLAB_CI, 'wb') as dest:
        with pkg_resources.resource_stream(__name__, _GITLAB_CI) as tpl:
            dest.write(tpl.read())


@runner.command()
@click.option(
    '--run/--no-run',
    is_flag=True,
    envvar='RENGA_RUNNER_RERUN',
    help='Run or only load the CWL and the job description.'
)
@click.option(
    '--job', envvar='RENGA_RUNNER_JOB', help='Job description in YAML.'
)
@pass_local_client
def rerun(repo, run, job):
    """Re-run existing workflow or tool using CWL runner."""
    from ._graph import Graph

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


def default_name():
    """Guess a default name from env."""
    project_slug = os.environ.get('CI_PROJECT_PATH_SLUG')
    env_slug = os.environ.get('CI_ENVIRONMENT_SLUG')

    if project_slug and env_slug:
        return project_slug + '-' + env_slug

    return uuid.uuid4().hex


def default_base_url():
    """Guess a default base url from env."""
    project_path = os.environ.get('CI_PROJECT_PATH')
    env_slug = os.environ.get('CI_ENVIRONMENT_SLUG')

    if project_path and env_slug:
        return '/{0}/{1}'.format(project_path, env_slug)

    return os.path.basename(os.getcwd())


@runner.command()
@click.option('--name', default=default_name)
@click.option('--network', envvar='RENGA_RUNNER_NETWORK', default='bridge')
@click.option(
    '--image',
    envvar='RENGA_RUNNER_IMAGE',
    default='jupyter/minimal-notebook:latest'
)
@click.option('--base-url', default=default_base_url)
@click.option('--repo-url', envvar='CI_REPOSITORY_URL')
@click.option(
    '--token', envvar='RENGA_NOTEBOOK_TOKEN', default=generate_notebook_token
)
@pass_local_client
def notebook(repo, name, network, image, base_url, repo_url, token):
    """Launch notebook in a container."""
    try:
        call(['docker', 'rm', '--force', name])
    except Exception:
        pass

    args = [
        'docker',
        'run',
        '-d',
        '--network',
        network,
        '--name',
        name,
        '--rm',
        '--label',
        'renga.notebook.token={0}'.format(token),
        '--label',
        'traefik.enable=true',
        '--label',
        'traefik.frontend.rule=PathPrefix:/{0}'.format(base_url.lstrip('/')),
        image,
    ] + generate_launch_args(
        token=token, base_url=base_url
    )

    call(args)

    if repo_url:
        call([
            'docker',
            'exec',
            name,
            'git',
            'clone',
            repo_url,
            os.path.join('/home/jovyan/', base_url.lstrip('/')),
        ])


@runner.command()
@click.option('--name', default=default_name)
@pass_local_client
def undeploy(repo, name):
    """Stop running deployment."""
    if not name:
        raise click.BadOptionUsage(
            'The name was not defined or detection failed.'
        )
    call(('docker', 'stop', name))
    call(('docker', 'rm', name))
