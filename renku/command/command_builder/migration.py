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
"""Command builder for migrations."""

from renku.command.command_builder.command import Command, check_finalized
from renku.core.migration.migrate import check_for_migration


class RequireMigration(Command):
    """Builder to check for migrations."""

    HOOK_ORDER = 3

    def __init__(self, builder: Command) -> None:
        """__init__ of RequireMigration."""
        self._builder = builder

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Check if migration is necessary."""
        check_for_migration()

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_pre_hook(self.HOOK_ORDER, self._pre_hook)

        return self._builder.build()
