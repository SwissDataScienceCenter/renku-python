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
from renku.core.metadata.database import Database
from renku.core.models.dataset import DatasetsProvenance


class DatabaseCommand(Command):
    """Builder to get a database connection."""

    PRE_ORDER = 4
    POST_ORDER = 5

    def __init__(self, builder: Command, write: bool = False, path: str = None, create: bool = False) -> None:
        self._builder = builder
        self._write = write
        self._path = path
        self._create = create

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Create a Database singleton."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")

        client = context["client"]

        self.database = Database.from_path(path=self._path or client.database_path)

        context["bindings"][Database] = self.database

        context["constructor_bindings"][DatasetsProvenance] = lambda: DatasetsProvenance(self.database)

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs) -> None:
        if self._write and not result.error:
            self.database.commit()

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_pre_hook(self.PRE_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.POST_ORDER, self._post_hook)

        return self._builder.build()
