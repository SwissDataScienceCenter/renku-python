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
"""Command builder for local object database."""


import json
import os
from typing import Optional

from packaging.version import Version

from renku.command.command_builder.command import Command, CommandResult, check_finalized
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.interface.storage import IStorageFactory
from renku.domain_model.project_context import project_context
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.infrastructure.gateway.database_gateway import DatabaseGateway
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from renku.infrastructure.gateway.project_gateway import ProjectGateway
from renku.infrastructure.storage.factory import StorageFactory


class DatabaseCommand(Command):
    """Builder to get a database connection."""

    PRE_ORDER = 4
    POST_ORDER = 5

    def __init__(self, builder: Command, write: bool = False, path: Optional[str] = None, create: bool = False) -> None:
        self._builder = builder
        self._write = write
        self._path = path
        self._create = create
        self.project_found: bool = False

    def _injection_pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Create a Database singleton."""
        from renku.version import __version__

        if not project_context.has_context():
            raise ValueError("Database builder needs a ProjectContext to be set.")

        project_context.push_path(path=self._path or project_context.path, save_changes=self._write)

        project_gateway = ProjectGateway()

        context["constructor_bindings"][IPlanGateway] = lambda: PlanGateway()
        context["constructor_bindings"][IActivityGateway] = lambda: ActivityGateway()
        context["constructor_bindings"][IDatabaseGateway] = lambda: DatabaseGateway()
        context["constructor_bindings"][IDatasetGateway] = lambda: DatasetGateway()
        context["constructor_bindings"][IProjectGateway] = lambda: project_gateway
        context["constructor_bindings"][IStorageFactory] = lambda: StorageFactory

        if int(os.environ.get("RENKU_SKIP_MIN_VERSION_CHECK", "0")) == 1:
            # NOTE: Used for unit tests
            return

        try:
            project = project_gateway.get_project()
            minimum_renku_version = Version(project.minimum_renku_version)
            self.project_found = True
        except (KeyError, ImportError, ValueError):
            try:
                with open(project_context.database_path / "project", "r") as f:
                    project = json.load(f)
                    min_version = project.get("minimum_renku_version")
                    if min_version is None:
                        return
                    minimum_renku_version = Version(min_version)
            except (KeyError, OSError, json.JSONDecodeError):
                # NOTE: We don't check minimum version if there's no project metadata available
                return

        current_version = Version(__version__)

        if current_version < minimum_renku_version:
            raise errors.MinimumVersionError(current_version, minimum_renku_version)

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs) -> None:
        from renku.domain_model.project import Project

        if self._write and self.project_found:
            # NOTE: Fetch project again in case it was updated (the current reference would be put of date)
            project_gateway = ProjectGateway()
            project = project_gateway.get_project()

            if Version(project.minimum_renku_version) < Version(Project.minimum_renku_version):
                # NOTE: update minimum renku version on write as migrations might happen on the fly
                project.minimum_renku_version = Project.minimum_renku_version

        project_context.pop_context()

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_injection_pre_hook(self.PRE_ORDER, self._injection_pre_hook)
        self._builder.add_post_hook(self.POST_ORDER, self._post_hook)

        return self._builder.build()
