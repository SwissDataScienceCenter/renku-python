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
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.serializers.cache import ProjectMigrationCheckRequest, ProjectMigrationCheckResponseRPC
from renku.service.views import result_response


class MigrationsCheckCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for migrations check endpoint."""

    REQUEST_SERIALIZER = ProjectMigrationCheckRequest()
    RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct migration check controller."""
        self.ctx = MigrationsCheckCtrl.REQUEST_SERIALIZER.load(request_data)
        super(MigrationsCheckCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        latest_version, project_version = migrations_versions().build().execute().output
        (
            migration_required,
            project_supported,
            template_update_possible,
            current_template_version,
            latest_template_version,
            automated_update,
            docker_update_possible,
        ) = (migrations_check().build().execute().output)

        return {
            "migration_required": migration_required,
            "template_update_possible": template_update_possible,
            "current_template_version": current_template_version,
            "latest_template_version": latest_template_version,
            "automated_template_update": automated_update,
            "docker_update_possible": docker_update_possible,
            "project_supported": project_supported,
            "project_version": project_version,
            "latest_version": latest_version,
        }

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(MigrationsCheckCtrl.RESPONSE_SERIALIZER, self.execute_op())
