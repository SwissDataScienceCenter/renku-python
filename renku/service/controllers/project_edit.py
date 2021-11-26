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
"""Renku service project edit controller."""
from renku.core.commands.project import edit_project_command
from renku.service.cache.models.job import Job
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.service.serializers.project import ProjectEditRequest, ProjectEditResponseRPC
from renku.service.views import result_response


class ProjectEditCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for project edit endpoint."""

    REQUEST_SERIALIZER = ProjectEditRequest()
    RESPONSE_SERIALIZER = ProjectEditResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a project edit controller."""
        self.ctx = ProjectEditCtrl.REQUEST_SERIALIZER.load(request_data)

        if self.ctx.get("commit_message") is None:
            self.ctx["commit_message"] = "service: project edit"

        super().__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = (
            edit_project_command()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                description=self.ctx.get("description"),
                creator=self.ctx.get("creator"),
                custom_metadata=self.ctx.get("custom_metadata"),
                keywords=self.ctx.get("keywords"),
            )
        )

        edited, warning = result.output
        return edited, warning

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        if isinstance(op_result, Job):
            return result_response(ProjectEditCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        edited, warning = op_result
        response = {
            "edited": edited,
            "warning": warning,
            "remote_branch": remote_branch,
        }

        return result_response(ProjectEditCtrl.RESPONSE_SERIALIZER, response)
