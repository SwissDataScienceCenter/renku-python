# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

Description
~~~~~~~~~~~

Authenticate with a remote Renku deployment. This command will bring up
a browser window where you can log in using your credentials. Renku CLI
receives and stores a secure token that will be used for future authentications.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.login:login
   :prog: renku login
   :nested: full

Examples
~~~~~~~~

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

By default, this command also logs you into gitlab server for project's
repository. You can use this method instead of creating an SSH key. This will
change the repository's remote URL to an endpoint in the deployment that adds
authentication to gitlab requests. Renku warns you each time that the remote
URL will change. To disable this warning, either pass ``--yes`` to the command
or set ``show_login_warning`` to ``false`` for the project or globally:

.. code-block:: console

    $ renku config set [--global] show_login_warning false

You can avoid the remote from being changed by passing ``--no-git`` option to
the login command.

.. note::

    Project's remote URL will be changed when logging in to Renku. Changes
    are undone when logging out from renku in the CLI. Original remote URL will
    be stored in a remote with name ``renku-backup-<remote-name>``.

Logging out from Renku removes the secure token from your system:

.. code-block:: console

    $ renku logout <endpoint>

If you don't specify an endpoint when logging out, credentials for all
endpoints are removed.

.. cheatsheet::
   :group: Misc
   :command: $ renku login --endpoint <URL>
   :description: Login to a Renku deployment for accessing private projects and dataset.
   :target: rp

.. cheatsheet::
   :group: Misc
   :command: $ renku logout --endpoint <URL>
   :description: Logout from a Renku deployment and clear locally-stored credentials.
   :target: rp

"""

import click

import renku.ui.cli.utils.color as color
from renku.ui.cli.utils.callback import ClickCallback


@click.command()
@click.argument("endpoint", required=False, default=None)
@click.option("--no-git", is_flag=True, default=False, help="Don't log in to gitlab.")
@click.option("--yes", is_flag=True, default=False, help="Do not warn users about remote URL change.")
def login(endpoint, no_git, yes):
    """Log in to the platform."""
    from renku.command.login import login_command

    communicator = ClickCallback()
    login_command().with_communicator(communicator).build().execute(endpoint=endpoint, git_login=not no_git, yes=yes)
    click.secho("Successfully logged in.", fg=color.GREEN)


@click.command()
@click.argument("endpoint", required=False, default=None)
def logout(endpoint):
    """Logout from the platform and delete credentials."""
    from renku.command.login import logout_command

    communicator = ClickCallback()
    logout_command().with_communicator(communicator).build().execute(endpoint=endpoint)
    click.secho("Successfully logged out.", fg=color.GREEN)


@click.command(hidden=True)
@click.option("--hostname", default=None, hidden=True, help="Remote hostname.")
@click.argument("command")
def credentials(command, hostname):
    """A git credential helper for returning renku user/token."""
    from renku.command.login import credentials_command

    communicator = ClickCallback()
    credentials_command().with_communicator(communicator).build().execute(command=command, hostname=hostname)
