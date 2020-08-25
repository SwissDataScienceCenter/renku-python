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
from renku.core.commands.migrate import migrations_check, migrations_versions
from renku.core.utils.contexts import chdir
from renku.service.controllers.remote_project import RemoteProject
from renku.service.serializers.cache import ProjectMigrationCheckRequest, ProjectMigrationCheckResponseRPC
from renku.service.views import result_response


class MigrationsCheckCtrl:
    """Controller for migrations check endpoint."""

    REQUEST_SERIALIZER = ProjectMigrationCheckRequest()
    RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct migration check controller."""
        self.ctx = MigrationsCheckCtrl.REQUEST_SERIALIZER.load(request_data)
        self.user = cache.ensure_user(user_data)

        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

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
        project = self.cache.get_project(self.user, self.ctx["project_id"])

        with chdir(project.abs_path):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        project = RemoteProject(self.user_data, self.request_data, branch=self.ctx.get("branch", "master"))

        with project.remote():
            return self.renku_op()

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        if "project_id" in self.ctx:
            return result_response(MigrationsCheckCtrl.RESPONSE_SERIALIZER, self.local())

        elif "git_url" in self.ctx:
            return result_response(MigrationsCheckCtrl.RESPONSE_SERIALIZER, self.remote())
