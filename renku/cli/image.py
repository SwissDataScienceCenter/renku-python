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

Configure the image registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, obtain an access token for the registry from GitLab by going to
``<gitlab-URL>/profile/personal_access_tokens``. Select only the
``read_registry`` scope and copy the access token.

.. code-block:: console

    $ open https://<gitlab-URL>/profile/personal_access_tokens
    $ export ACCESS_TOKEN=<copy-from-browser>

Find your project's registry path by going to
``<gitlab-url>/<namespace>/<project>/container_registry``. The string following
the docker push command is the ``registry-path`` for the project.

.. code-block:: console

    $ open https://<gitlab-url>/<namespace>/<project>/container_registry
    $ renku config registry https://oauth2:$ACCESS_TOKEN@<registry-path>

You can use any registry with manual authentication step using Docker command
line.

.. code-block:: console

    $ docker login docker.io
    $ renku config registry https://docker.io

Pull image
~~~~~~~~~~

If the image has indeed been built and pushed to the registry, you should be
able to fetch it with:

.. code-block:: console

    $ renku image pull

This pulls an image that was built for the current commit. You can also fetch
an image built for a specific commit with:

.. code-block:: console

    # renku image pull --revision <ref-name>
    $ renku image pull --revision HEAD~1

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

    repo = client.git
    sha = repo.rev_parse(revision).hexsha
    short_sha = repo.git.rev_parse(sha, short=7)

    image = '{registry}:{short_sha}'.format(
        registry=registry_url.image, short_sha=short_sha
    )

    result = subprocess.run(['docker', 'image', 'pull', image])
    if result.returncode != 0:
        raise click.ClickException(
            'The image "{image}" was not pulled.\n\n'
            'Push the repository to the server or build the image manually:\n'
            '\n\tdocker build -t {image} .'.format(image=image)
        )
