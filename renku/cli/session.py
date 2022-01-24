# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Manage local interactive sessions."""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core import errors


@click.group()
def session():
    """Session commands."""
    pass


@session.command("list")
def list():
    """List local interactive sessions."""
    from renku.core.commands.session import session_list_command

    result = session_list_command().build().execute()
    click.echo(result.output)


@session.command("start")
@click.argument("image", metavar="<image_name>", required=False)
def start(image):
    """Start a local interactive sessions."""
    from renku.core.commands.session import session_start_command

    communicator = ClickCallback()
    result = session_start_command().with_communicator(communicator).build().execute(image_name=image)
    click.echo(result.output)


@session.command("stop")
@click.argument("session_name", metavar="<name>", required=False)
@click.option("stop_all", "--all", is_flag=True, help="Stops all the running containers.")
def stop(session_name, stop_all):
    """Stop a local interactive sessions."""
    from renku.core.commands.session import session_stop_command

    if not stop_all and session_name is None:
        raise errors.ParameterError("Please specify either a session ID or the '--all' flag.")

    session_stop_command().build().execute(session_name=session_name, stop_all=stop_all)
    if stop_all:
        click.echo("All running local interactive sessions for this project have been stopped.")
    else:
        click.echo(f"Local interactive '{session_name}' has been successfully stopped.")
