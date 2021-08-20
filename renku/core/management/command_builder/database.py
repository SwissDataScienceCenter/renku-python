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
"""Command builder for local object database."""


from renku.core.management.command_builder.command import Command, CommandResult, check_finalized
from renku.core.management.command_builder.database_dispatcher import DatabaseDispatcher
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.metadata.gateway.activity_gateway import ActivityGateway
from renku.core.metadata.gateway.database_gateway import DatabaseGateway
from renku.core.metadata.gateway.dataset_gateway import DatasetGateway
from renku.core.metadata.gateway.plan_gateway import PlanGateway
from renku.core.metadata.gateway.project_gateway import ProjectGateway


class DatabaseCommand(Command):
    """Builder to get a database connection."""

    PRE_ORDER = 4
    POST_ORDER = 5

    def __init__(self, builder: Command, write: bool = False, path: str = None, create: bool = False) -> None:
        self._builder = builder
        self._write = write
        self._path = path
        self._create = create

    def _injection_pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Create a Database singleton."""
        if "client_dispatcher" not in context:
            raise ValueError("Database builder needs a IClientDispatcher to be set.")

        client = context["client_dispatcher"].current_client

        self.dispatcher = DatabaseDispatcher()
        self.dispatcher.push_database_to_stack(path=self._path or client.database_path, commit=self._write)

        context["bindings"][IDatabaseDispatcher] = self.dispatcher

        context["constructor_bindings"][IPlanGateway] = lambda: PlanGateway()
        context["constructor_bindings"][IActivityGateway] = lambda: ActivityGateway()
        context["constructor_bindings"][IDatabaseGateway] = lambda: DatabaseGateway()
        context["constructor_bindings"][IDatasetGateway] = lambda: DatasetGateway()
        context["constructor_bindings"][IProjectGateway] = lambda: ProjectGateway()

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs) -> None:
        self.dispatcher.finalize_dispatcher()

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_injection_pre_hook(self.PRE_ORDER, self._injection_pre_hook)
        self._builder.add_post_hook(self.POST_ORDER, self._post_hook)

        return self._builder.build()
