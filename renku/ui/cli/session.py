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

The command first looks for a local image to use. If a local image isn't found, it
searches the remote Renku deployment (if any) and pulls the image if it exists.
Finally, it prompts the user to build the image locally if no image is found. You
can force the image to always be built by using the ``--force-build`` flag.

This command accepts a subset of arguments of the ``docker run`` command. See
its help for the list of supported arguments: ``renku session start --help``.
Accepted values are the same as the ``docker run`` command unless stated
otherwise.

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

SSH connections to remote sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can connect via SSH to remote (Renkulab) sessions, if your project supports it.

To see if your project supports SSH, you can run ``renku project show`` and check the
``SSH Supported`` flag. If your project doesn't support SSH, update the project template
or contact the template maintainer to enable SSH support on the template.

You can start a session with SSH support using:

.. code-block:: console

    $ renku session start -p renkulab --ssh
    Your system is not set up for SSH connections to Renkulab. Would you like to set it up? [y/N]: y
    [...]
    Session <session-id> successfully started, use 'renku session open --ssh <session-id>' or 'ssh <session-id>' to
    connect to it

This will create SSH keys for you and setup SSH configuration for connecting to the renku deployment.
You can then use the SSH connection name (``ssh renkulab.io-myproject-session-id`` in the example)
to connect to the session or in tools such as VSCode.

.. note::

   If you need to recreate the generated SSH keys, or you want to use existing keys instead,
   you can use the ``renku session ssh-setup`` command to perform this step manually. See
   the help of the command for more details.

Alternatively, you can use ``renku session open --ssh <session_id>`` to directly open an SSH
connection to the session.

You can see the SSH connection name using ``renku session ls``.

SSH config for specific sessions is removed when the session is stopped.

Managing active sessions
~~~~~~~~~~~~~~~~~~~~~~~~

The ``session`` command can be used to also list, stop and open active sessions.
If the provider supports sessions hibernation, this command allows pausing and resuming
sessions as well.
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

If a provider supports session hibernation (e.g. ``renkulab`` provider) you can pause a session using
its ``ID``:

.. code-block:: console

    $ renku session pause renku-test-e4fe76cc

A paused session can be later resumed:

.. code-block:: console

    $ renku session resume renku-test-e4fe76cc

.. note::

   Session ``ID`` doesn't need to be passed to the above commands if there is only one interactive session available.


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
   :command: $ renku session pause <name>
   :description: Pause the specified session.
   :target: rp

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session resume <name>
   :description: Resume the specified paused session.
   :target: rp

.. cheatsheet::
   :group: Managing Interactive Sessions
   :command: $ renku session stop <name>
   :description: Stop the specified session.
   :target: rp

"""

import click
from lazy_object_proxy import Proxy

from renku.command.format.session import SESSION_COLUMNS, SESSION_FORMATS
from renku.command.util import WARNING
from renku.core import errors
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.click import (
    shell_complete_hibernating_session_providers,
    shell_complete_session_providers,
    shell_complete_sessions,
)
from renku.ui.cli.utils.plugins import (
    get_supported_hibernating_session_providers_names,
    get_supported_session_providers_names,
)


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
    shell_complete=shell_complete_session_providers,
    default=None,
    help="Backend to use for listing interactive sessions.",
)
@click.option(
    "--columns",
    type=click.STRING,
    default=None,
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(SESSION_COLUMNS.keys())),
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(list(SESSION_FORMATS.keys())), default="log", help="Choose an output format."
)
def list_sessions(provider, columns, format):
    """List interactive sessions."""
    from renku.command.session import session_list_command

    result = session_list_command().build().execute(provider=provider)

    click.echo(SESSION_FORMATS[format](result.output.sessions, columns=columns))

    if result.output.warning_messages:
        click.echo()
        if result.output.all_local and result.output.sessions:
            click.echo(WARNING + "Only showing sessions from local provider")
        for message in result.output.warning_messages:
            click.echo(WARNING + message)


def session_start_provider_options(*_, **__):
    """Sets session provider options groups on the session start command."""
    from renku.core.plugin.session import get_supported_session_providers
    from renku.ui.cli.utils.click import create_options

    providers = [p for p in get_supported_session_providers() if p.get_start_parameters()]
    return create_options(providers=providers, parameter_function="get_start_parameters")


@session.command
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    shell_complete=shell_complete_session_providers,
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
@click.option(
    "--gpu",
    type=click.STRING,
    metavar="<GPU quota>",
    help="Number of GPU devices to add to the container ('all' to pass all GPUs).",
)
@click.option("--memory", type=click.STRING, metavar="<memory size>", help="Amount of memory required for the session.")
@session_start_provider_options()
def start(provider, config, image, cpu, disk, gpu, memory, **kwargs):
    """Start an interactive session."""
    from renku.command.session import session_start_command

    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    communicator = ClickCallback()
    session_start_command().with_communicator(communicator).build().execute(
        provider=provider,
        config_path=config,
        image_name=image,
        cpu_request=cpu,
        mem_request=memory,
        disk_request=disk,
        gpu_request=gpu,
        **kwargs,
    )


@session.command
@click.argument("session_name", metavar="<name>", required=False, default=None, shell_complete=shell_complete_sessions)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    default=None,
    shell_complete=shell_complete_session_providers,
    help="Session provider to use.",
)
@click.option("stop_all", "-a", "--all", is_flag=True, help="Stops all the running containers.")
def stop(session_name, stop_all, provider):
    """Stop an interactive session."""
    from renku.command.session import session_stop_command

    if stop_all and session_name:
        raise errors.ParameterError("Cannot specify a session ID with the '-a/--all' flag.")

    communicator = ClickCallback()
    session_stop_command().with_communicator(communicator).build().execute(
        session_name=session_name, stop_all=stop_all, provider=provider
    )
    if stop_all:
        click.echo("All running interactive sessions for this project have been stopped.")
    elif session_name:
        click.echo(f"Interactive session '{session_name}' has been successfully stopped.")
    else:
        click.echo("Interactive session has been successfully stopped.")


def session_open_provider_options(*_, **__):
    """Sets session provider option groups on the session open command."""
    from renku.core.plugin.session import get_supported_session_providers
    from renku.ui.cli.utils.click import create_options

    providers = [p for p in get_supported_session_providers() if p.get_open_parameters()]  # type: ignore
    return create_options(providers=providers, parameter_function="get_open_parameters")


@session.command
@click.argument("session_name", metavar="<name>", required=False, default=None, shell_complete=shell_complete_sessions)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_session_providers_names)),
    shell_complete=shell_complete_session_providers,
    default=None,
    help="Session provider to use.",
)
@session_open_provider_options()
def open(session_name, provider, **kwargs):
    """Open an interactive session."""
    from renku.command.session import session_open_command

    session_open_command().build().execute(session_name=session_name, provider=provider, **kwargs)


@session.command("ssh-setup")
@click.option(
    "existing_key",
    "-k",
    "--existing-key",
    type=click.Path(exists=True, dir_okay=False),
    metavar="<private key file>",
    help="Existing private key to use.",
)
@click.option("--force", is_flag=True, help="Overwrite existing keys/config.")
def ssh_setup(existing_key, force):
    """Generate keys and configuration for SSH connections into sessions.

    Note that this will not add any keys to a specific project, adding keys to a project
    has to be done manually or through the renku session start command by using the --ssh flag.
    """
    from renku.command.session import ssh_setup_command

    communicator = ClickCallback()
    ssh_setup_command().with_communicator(communicator).build().execute(existing_key=existing_key, force=force)


@session.command
@click.argument("session_name", metavar="<name>", required=False, default=None, shell_complete=shell_complete_sessions)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_hibernating_session_providers_names)),
    default=None,
    shell_complete=shell_complete_hibernating_session_providers,
    help="Session provider to use.",
)
def pause(session_name, provider):
    """Pause an interactive session."""
    from renku.command.session import session_pause_command

    communicator = ClickCallback()
    session_pause_command().with_communicator(communicator).build().execute(
        session_name=session_name, provider=provider
    )

    session_message = f"session '{session_name}'" if session_name else "session"
    click.echo(f"Interactive {session_message} has been paused.")


@session.command
@click.argument("session_name", metavar="<name>", required=False, default=None, shell_complete=shell_complete_sessions)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(get_supported_hibernating_session_providers_names)),
    default=None,
    shell_complete=shell_complete_hibernating_session_providers,
    help="Session provider to use.",
)
def resume(session_name, provider):
    """Resume a paused session."""
    from renku.command.session import session_resume_command

    communicator = ClickCallback()
    session_resume_command().with_communicator(communicator).build().execute(
        session_name=session_name, provider=provider
    )

    session_message = f"session '{session_name}'" if session_name else "session"
    click.echo(f"Interactive {session_message} has been resumed.")
