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
from subprocess import call

import click
import yaml

from ._client import pass_local_client

_GITLAB_CI = '.gitlab-ci.yml'
_DOCKERFILE = 'Dockerfile'
_REQUIREMENTS = 'requirements.txt'
CI_TEMPLATES = [_GITLAB_CI, _DOCKERFILE, _REQUIREMENTS]


@click.group()
def runner():
    """Simplify running of CI scripts."""


@runner.command()
@pass_local_client
def template(client):
    """Render templated configuration files."""
    import pkg_resources

    # create the templated files
    for tpl_file in CI_TEMPLATES:
        tpl_path = client.path / tpl_file
        with tpl_path.open('wb') as dest:
            with pkg_resources.resource_stream(__name__, tpl_file) as tpl:
                dest.write(tpl.read())


@runner.command()
@click.option(
    '--run/--no-run',
    is_flag=True,
    envvar='RENKU_RUNNER_RERUN',
    help='Run or only load the CWL and the job description.'
)
@click.option(
    '--job', envvar='RENKU_RUNNER_JOB', help='Job description in YAML.'
)
@pass_local_client
def rerun(client, run, job):
    """Re-run existing workflow or tool using CWL runner."""
    from renku.models.provenance import ProcessRun, from_git_commit
    activity = from_git_commit(commit=client.repo.head.commit, client=client)

    if not isinstance(activity, ProcessRun):
        click.secho('No tool was found.', fg='red', file=sys.stderr)
        return

    try:
        args = ['cwl-runner', activity.path]
        if job:
            job_file = tempfile.NamedTemporaryFile(
                suffix='.yml', dir=os.getcwd(), delete=False
            )
            args.append(job_file.name)

            with job_file as fp:
                yaml.dump(yaml.load(job), stream=fp, encoding='utf-8')

        if run:
            return call(args, cwd=os.getcwd())
    finally:
        if job:
            os.unlink(job_file.name)
