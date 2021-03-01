# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 Swiss Data Science Center (SDSC)
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
"""Logging in to a Renku deployment.

You can use ``renku login`` command to authenticate with a remote Renku
deployment. This command will bring up a browser window where you can log in
using your credentials. Renku CLI receives and stores a secure token that will
be used for future authentications.

.. code-block:: console

    $ renku login <endpoint>

Parameter ``endpoint`` is the URL of the Renku deployment that you want to
authenticate with (e.g. ``renkulab.io``). You can either pass this parameter on
the command-line or set it once in project's configuration:

.. code-block:: console

    $ renku config set endpoint <endpoint>

.. note::

    The secure token is stored in plain-text in Renku's global configuration
    file on your home directory (``~/.renku/renku.ini``). Renku changes access
    rights of this file to be readable only by you. This token exists only on
    your system and won't be pushed to a remote server.

Logging out from Renku removes the secure token from your system:

.. code-block:: console

    $ renku logout <endpoint>

If you don't specify an endpoint when logging out, credentials for all
endpoints are removed.
"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.login import login_command, logout_command


@click.command()
@click.argument("endpoint", required=False, default=None)
def login(endpoint):
    """Log in to the platform."""
    communicator = ClickCallback()
    login_command().with_communicator(communicator).build().execute(endpoint=endpoint)
    click.secho("Successfully logged in.", fg="green")


@click.command()
@click.argument("endpoint", required=False, default=None)
def logout(endpoint):
    """Logout from the platform and delete credentials."""
    communicator = ClickCallback()
    logout_command().with_communicator(communicator).build().execute(endpoint=endpoint)
    click.secho("Successfully logged out.", fg="green")
