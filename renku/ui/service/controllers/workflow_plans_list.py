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
"""Renku service plans list controller."""

from renku.command.command_builder.command import Command
from renku.core.workflow.plan import get_plans_with_metadata
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.workflows import WorkflowPlansListRequest, WorkflowPlansListResponseRPC
from renku.ui.service.views import result_response


class WorkflowPlansListCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for plans list endpoint."""

    REQUEST_SERIALIZER = WorkflowPlansListRequest()
    RESPONSE_SERIALIZER = WorkflowPlansListResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a plans list controller."""
        self.ctx = WorkflowPlansListCtrl.REQUEST_SERIALIZER.load(request_data)
        super(WorkflowPlansListCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        plan_list_command = Command().command(get_plans_with_metadata).with_database().require_migration()
        result = plan_list_command.build().execute()
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["plans"] = self.execute_op()
        return result_response(WorkflowPlansListCtrl.RESPONSE_SERIALIZER, self.ctx)
