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
"""Manipulate images related to the Renku project.

Pull image
~~~~~~~~~~

If you have an existing project registered on GitLab with a Docker registry
enabled you can simply type:

.. code-block:: console

    $ renku image pull

This pulls an image that was built for the current commit.
"""

import subprocess

import click

from ._client import pass_local_client
from ._docker import detect_registry_url


@click.group()
def image():
    """Manipulate images related to the Renku project."""


@image.command()
@click.option('--revision', default='HEAD')
@click.option(
    '--auto-login/--no-auto-login',
    default=True,
    help='Enable/disable auto login for Docker registry'
)
@pass_local_client
def pull(client, revision, auto_login):
    """Pull an existing image from the project registry."""
    registry_url = detect_registry_url(client, auto_login=auto_login)
    registry = registry_url.split('://', 1)[-1]  # remove http(s):// prefix

    repo = client.git
    sha = repo.rev_parse(revision).hexsha
    short_sha = repo.git.rev_parse(sha, short=7)

    image = '{registry}:{short_sha}'.format(
        registry=registry, short_sha=short_sha
    ).lower()

    result = subprocess.run(['docker', 'image', 'pull', image])
    if result.returncode != 0:
        raise click.ClickException(
            'The image "{image}" was not pulled.\n\n'
            'Push the repository to the server or build the image manually:\n'
            '\n\tdocker build -t {image} .'.format(image=image)
        )
