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
"""Install and uninstall Git hooks."""

from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.githooks import install, uninstall
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication


@inject.autoparams()
def _install_githooks(force, client_dispatcher: IClientDispatcher):
    """Install Git hooks."""
    client = client_dispatcher.current_client

    warning_messages = install(force=force, repository=client.repository)
    if warning_messages:
        for message in warning_messages:
            communication.warn(message)


def install_githooks_command():
    """Command to install Git hooks."""
    return Command().command(_install_githooks)


def uninstall_githooks_command():
    """Command to uninstall Git hooks."""
    return Command().command(uninstall)
