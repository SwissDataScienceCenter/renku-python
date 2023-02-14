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
from typing import Dict, cast

from renku.command.project import edit_project_command
from renku.core.util.util import NO_VALUE
from renku.ui.service.cache.models.job import Job
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.serializers.project import ProjectEditRequest, ProjectEditResponseRPC
from renku.ui.service.views import result_response


class ProjectEditCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for project edit endpoint."""

    REQUEST_SERIALIZER = ProjectEditRequest()
    RESPONSE_SERIALIZER = ProjectEditResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a project edit controller."""
        self.ctx = cast(Dict, ProjectEditCtrl.REQUEST_SERIALIZER.load(request_data))

        if self.ctx.get("commit_message") is None:
            self.ctx["commit_message"] = "service: project edit"

        super().__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        if "description" in self.ctx:
            description = self.ctx.get("description")
        else:
            description = NO_VALUE

        if "creator" in self.ctx:
            creator = self.ctx.get("creator")
        else:
            creator = NO_VALUE

        if "custom_metadata" in self.ctx:
            custom_metadata = self.ctx.get("custom_metadata")
        else:
            custom_metadata = NO_VALUE

        if "custom_metadata_source" in self.ctx:
            custom_metadata_source = self.ctx.get("custom_metadata_source")
        else:
            custom_metadata_source = NO_VALUE

        if custom_metadata_source is NO_VALUE and custom_metadata is not NO_VALUE:
            custom_metadata_source = "renku"

        if "keywords" in self.ctx:
            keywords = self.ctx.get("keywords")
        else:
            keywords = NO_VALUE

        result = (
            edit_project_command()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                description=description,
                creator=creator,
                custom_metadata=custom_metadata,
                custom_metadata_source=custom_metadata_source,
                keywords=keywords,
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
