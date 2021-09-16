# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Renku project gateway interface."""

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.models.project import Project


class ProjectGateway(IProjectGateway):
    """Gateway for project database operations."""

    database_dispatcher = inject.attr(IDatabaseDispatcher)

    def get_project(self) -> Project:
        """Get project metadata."""
        try:
            return self.database_dispatcher.current_database["project"]
        except KeyError as e:
            raise ValueError() from e

    def update_project(self, project: Project):
        """Update project metadata."""
        from renku import __version__

        database = self.database_dispatcher.current_database

        try:
            if database["project"]:
                database.remove_root_object("project")
        except KeyError:
            pass

        project.agent_version = __version__

        database.add_root_object("project", project)
