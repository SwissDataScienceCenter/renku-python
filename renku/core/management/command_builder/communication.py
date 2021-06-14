# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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

from renku.core.management.command_builder.command import Command, check_finalized
from renku.core.utils import communication


class Communicator(Command):
    """Hook for logging and interaction with user."""

    DEFAULT_ORDER = 2

    def __init__(self, builder, communicator):
        """__init__ of Communicator.

        :param communicator: Instance of CommunicationCallback.
        """
        self._builder = builder
        self._communicator = communicator

    def _pre_hook(self, builder, context, *args, **kwargs):
        communication.subscribe(self._communicator)

    def _post_hook(self, builder, context, result, *args, **kwargs):
        communication.unsubscribe(self._communicator)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.DEFAULT_ORDER, self._post_hook)

        return self._builder.build()
