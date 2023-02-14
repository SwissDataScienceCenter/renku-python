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
"""Renku service plans show controller."""

from renku.command.workflow import export_workflow_command
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.workflows import WorkflowPlansExportRequest, WorkflowPlansExportResponseRPC
from renku.ui.service.views import result_response


class WorkflowPlansExportCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for plan show endpoint."""

    REQUEST_SERIALIZER = WorkflowPlansExportRequest()
    RESPONSE_SERIALIZER = WorkflowPlansExportResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a workflow plan show controller."""
        self.ctx = WorkflowPlansExportCtrl.REQUEST_SERIALIZER.load(request_data)
        super(WorkflowPlansExportCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = (
            export_workflow_command()
            .build()
            .execute(
                name_or_id=self.ctx["plan_id"],
                format=self.ctx["format"].value,
                output=None,
                values=self.ctx["values"],
                basedir=".",
                resolve_paths=False,
                nest_workflows=True,
            )
        )
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        result = self.execute_op()
        return result_response(WorkflowPlansExportCtrl.RESPONSE_SERIALIZER, result)
