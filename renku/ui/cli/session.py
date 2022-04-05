# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Manage interactive sessions."""

import click
from lazy_object_proxy import Proxy

from renku.command.format.session import SESSION_FORMATS
from renku.core import errors
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.plugins import supported_session_providers


@click.group()
def session():
    """Session commands."""
    pass


@session.command("list")
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(supported_session_providers)),
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
    type=click.Choice(Proxy(supported_session_providers)),
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
    """Start a interactive sessions."""
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
    type=click.Choice(Proxy(supported_session_providers)),
    default=None,
    help="Session provider to use.",
)
@click.option("stop_all", "--all", is_flag=True, help="Stops all the running containers.")
def stop(session_name, stop_all, provider):
    """Stop a interactive sessions."""
    from renku.command.session import session_stop_command

    if not stop_all and session_name is None:
        raise errors.ParameterError("Please specify either a session ID or the '--all' flag.")

    session_stop_command().build().execute(session_name=session_name, stop_all=stop_all, provider=provider)
    if stop_all:
        click.echo("All running interactive sessions for this project have been stopped.")
    else:
        click.echo(f"Interactive '{session_name}' has been successfully stopped.")


@session.command("open")
@click.argument("session_name", metavar="<name>", required=False)
@click.option(
    "provider",
    "-p",
    "--provider",
    type=click.Choice(Proxy(supported_session_providers)),
    default=None,
    help="Session provider to use.",
)
def open(session_name, provider):
    """Stop a interactive sessions."""
    from renku.command.session import session_open_command

    session_open_command().build().execute(session_name=session_name, provider=provider)
