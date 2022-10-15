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
"""Command builder for communication."""

from renku.command.command_builder.command import Command, CommandResult, check_finalized
from renku.core.util import communication


class Communicator(Command):
    """Hook for logging and interaction with user."""

    HOOK_ORDER = 2

    def __init__(self, builder: Command, communicator: communication.CommunicationCallback) -> None:
        """__init__ of Communicator.

        Args:
            communicator: Instance of CommunicationCallback.
        """
        self._builder = builder
        self._communicator = communicator

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        communication.subscribe(self._communicator)

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs) -> None:
        communication.unsubscribe(self._communicator)

    @check_finalized
    def build(self) -> Command:
        """Build the command.

        Returns:
            Command: Finalized version of this command.
        """
        self._builder.add_pre_hook(self.HOOK_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.HOOK_ORDER, self._post_hook)

        return self._builder.build()
