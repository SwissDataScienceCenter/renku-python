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
"""Command builder for locking."""

from renku.command.command_builder.command import Command, check_finalized
from renku.domain_model.project_context import project_context


class ProjectLock(Command):
    """Builder to get a project wide lock."""

    HOOK_ORDER = 5

    def __init__(self, builder: Command) -> None:
        """__init__ of ProjectLock."""
        self._builder = builder

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Lock the project."""
        if "stack" not in context:
            raise ValueError(f"{self.__class__.__name__} builder needs a stack to be set.")

        context["stack"].enter_context(project_context.lock)

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_pre_hook(self.HOOK_ORDER, self._pre_hook)

        return self._builder.build()


class DatasetLock(ProjectLock):
    """Builder to lock on a dataset."""

    pass
