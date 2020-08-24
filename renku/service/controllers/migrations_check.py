# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service migrations check controller."""
from renku.core.commands.migrate import migrations_versions, migrations_check
from renku.core.utils.contexts import chdir
from renku.service.controllers.remote_project import RemoteProject
from renku.service.serializers.cache import (
    ProjectMigrationCheckResponseRPC,
    ProjectMigrationCheckRequest)
from renku.service.views import result_response


class MigrationsCheckCtrl:
    """Controller for migrations check endpoint."""

    REQUEST_SERIALIZER = ProjectMigrationCheckRequest()
    RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct migration check controller."""
        self.cache = cache
        self.user = cache.ensure_user(user_data)
        self.request = self.REQUEST_SERIALIZER.load(request_data)

    def renku_op(self):
        """Renku operation for the controller."""
        latest_version, project_version = migrations_versions()
        migration_required, project_supported = migrations_check()

        return {
            "migration_required": migration_required,
            "project_supported": project_supported,
            "project_version": project_version,
            "latest_version": latest_version,
        }

    def local(self):
        """Execute renku operation against service cache."""
        project = self.cache.get_project(self.user, self.request["project_id"])

        with chdir(project.abs_path):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        project = RemoteProject(self.request["project_remote"], branch=self.request.get("project_branch", "master"))

        with project.remote():
            return self.renku_op()

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        if "project_id" in self.request:
            return result_response(self.RESPONSE_SERIALIZER, self.local())

        elif "project_remote" in self.request:
            return result_response(self.RESPONSE_SERIALIZER, self.remote())
