# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Manage interactive sessions.

Description
~~~~~~~~~~~

Manage sessions through the command line interface by starting and stopping them.
It's possible to list the current sessions and to connect to them.

Currently, two providers are supported: ``docker`` and ``renkulab``. More on this
later.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: special

.. click:: renku.ui.cli.session:session
   :prog: renku session
   :nested: full

Docker provider
~~~~~~~~~~~~~~~

The ``docker`` provider will take the current state of the repository, build a ``docker``
image (if one does not already exist) and then launch a session with this image. In
addition to this, the ``docker`` provider will mount the local repository inside
the ``docker`` container so that changes made in the session are immediately reflected
on the host where the session was originally started from.

Please note that in order to use this provider `Docker <https://docs.docker.com/>`_
is expected to be installed and available on your computer. In addition, using
this command from within a Renku interactive session started from the Renku website
is not possible. This command is envisioned as a means for users to quickly test
and check their sessions locally without going to a Renku deployment and launching
a session there, or in the case where they simply have no access to a Renku deployment.

.. code-block:: console

    $ renku session start -p docker

The command first looks for a local image to use. If a local image isn't found, it searches the remote Renku deployment
(if any) and pulls the image if it exists. Finally, it prompts the user to build the image locally if no image is found.

Renkulab provider
~~~~~~~~~~~~~~~~~

The ``renkulab`` provider will launch a regular interactive session
in the Renku deployment that hosts the current project. If the project has not
been uploaded/created in a Renku deployment then this provider will not be able
to launch a session. This provider is identical to going through the Renku website
and launching a session "manually" by selecting the project, commit, branch, etc.

Please note that there are a few limitations with the ``renkulab`` provider:

* If the user is not logged in (using the ``renku login`` command) then sessions
  can only be launched if the specific Renku deployment supports anonymous sessions.

* When launching anonymous sessions local changes cannot be reflected in them and
  changes made inside the session cannot be saved nor downloaded locally. This feature
  should be used only for adhoc exploration or work that can be discarded when
  the session is closed. The CLI will print a warning every time an anonymous session
  is launched.

* Changes made inside the interactive session are not immediately reflected locally,
  users should ``git pull`` any changes made inside an interactive session to get the
  same changes locally.

* Local changes can only be reflected in the interactive session if they are committed
  and pushed to the git repository. When launching a session and uncommitted or unpushed
  changes are present, the user will be prompted to confirm whether Renku should
  commit and push the changes before a session is launched. The session will launch
  only if the changes are committed and pushed.

.. code-block:: console

    $ renku session start -p renkulab

Managing active sessions
~~~~~~~~~~~~~~~~~~~~~~~~

The ``session`` command can be used to also list, stop and open active sessions.
In order to see active sessions (from any provider) run the following command:

.. code-block:: console

    $ renku session ls -p renkulab
    ID                   STATUS    URL
    -------------------  --------  -------------------------------------------------
    renku-test-e4fe76cc  running   https://dev.renku.ch/sessions/renku-test-e4fe76cc

An active session can be opened by using its ``ID`` from the list above. For example, the ``open``
command below will open the single active session in the browser.

.. code-block:: console

    $ renku session open renku-test-e4fe76cc

An active session can be stopped by using the ``stop`` command and the ``ID`` from the list of
active sessions.

.. code-block:: console

    $ renku session stop renku-test-e4fe76cc

The command ``renku session stop --all`` will stop all active sessions regardless of the provider.

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session start --provider renkulab
   :description: Start an interactive session on the remote Renku deployment.
   :target: rp

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session ls
   :description: List all active sessions.
   :target: rp

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session open <name>
   :description: Open a browser tab and connect to a running session.
   :target: rp

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session stop <name>
   :description: Stop the specified session.
   :target: rp

"""

import click
from lazy_object_proxy import Proxy

from renku.command.format.session import SESSION_FORMATS
from renku.core import errors
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.plugins import get_supported_session_providers_names


@click.group()
def session():
    """Session commands."""
    pass


@session.command("ls")
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    default=None,
    help="Backend to use for listing interactive sessions.",
)
@click.option(
    "config",
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    metavar="<config file>",
    help="YAML file containing configuration for the provider.",
)
@click.option(
    "--format", type=click.Choice(list(SESSION_FORMATS.keys())), default="tabular", help="Choose an output format."
)
def list_sessions(provider, config, format):
    """List interactive sessions."""
    from renku.command.session import session_list_command

    result = session_list_command().build().execute(provider=provider, config_path=config)
    click.echo(SESSION_FORMATS[format](result.output))


@session.command("start")
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    default="docker",
    show_default=True,
    help="Backend to use for creating an interactive session.",
)
@click.option(
    "config",
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    metavar="<config file>",
    help="YAML file containing configuration for the provider.",
)
@click.option("--image", type=click.STRING, metavar="<image_name>", help="Docker image to use for the session.")
@click.option("--cpu", type=click.FLOAT, metavar="<cpu quota>", help="CPUs quota for the session.")
@click.option("--disk", type=click.STRING, metavar="<disk size>", help="Amount of disk space required for the session.")
@click.option("--gpu", type=click.STRING, metavar="<GPU quota>", help="GPU quota for the session.")
@click.option("--memory", type=click.STRING, metavar="<memory size>", help="Amount of memory required for the session.")
def start(provider, config, image, cpu, disk, gpu, memory):
    """Start an interactive session."""
    from renku.command.session import session_start_command

    communicator = ClickCallback()
    result = (
        session_start_command()
        .with_communicator(communicator)
        .build()
        .execute(
            provider=provider,
            config_path=config,
            image_name=image,
            cpu_request=cpu,
            mem_request=memory,
            disk_request=disk,
            gpu_request=gpu,
        )
    )
    click.echo(result.output)


@session.command("stop")
@click.argument("session_name", metavar="<name>", required=False)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    default=None,
    help="Session provider to use.",
)
@click.option("stop_all", "-a", "--all", is_flag=True, help="Stops all the running containers.")
def stop(session_name, stop_all, provider):
    """Stop an interactive session."""
    from renku.command.session import session_stop_command

    if not stop_all and session_name is None:
        raise errors.ParameterError("Please specify either a session ID or the '-a/--all' flag.")
    elif stop_all and session_name:
        raise errors.ParameterError("Cannot specify a session ID with the '-a/--all' flag.")

    communicator = ClickCallback()
    session_stop_command().with_communicator(communicator).build().execute(
        session_name=session_name, stop_all=stop_all, provider=provider
    )
    if stop_all:
        click.echo("All running interactive sessions for this project have been stopped.")
    else:
        click.echo(f"Interactive session '{session_name}' has been successfully stopped.")


@session.command("open")
@click.argument("session_name", metavar="<name>", required=True)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    default=None,
    help="Session provider to use.",
)
def open(session_name, provider):
    """Open an interactive session."""
    from renku.command.session import session_open_command

    session_open_command().build().execute(session_name=session_name, provider=provider)
