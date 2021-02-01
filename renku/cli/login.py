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

TODO: Write Documentation
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

    # TODO: Print a warning that token is stored in plain-text


@click.command()
def logout():
    """Logout from the platform and delete credentials."""
    communicator = ClickCallback()
    logout_command().with_communicator(communicator).build().execute()
