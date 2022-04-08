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
"""Renku service project show controller."""
from renku.command.project import show_project_command
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.project import ProjectShowRequest, ProjectShowResponseRPC
from renku.ui.service.views import result_response


class ProjectShowCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for project show endpoint."""

    REQUEST_SERIALIZER = ProjectShowRequest()
    RESPONSE_SERIALIZER = ProjectShowResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a project edit controller."""
        self.ctx = ProjectShowCtrl.REQUEST_SERIALIZER.load(request_data)

        super().__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = show_project_command().build().execute()
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        result = self.execute_op()
        return result_response(ProjectShowCtrl.RESPONSE_SERIALIZER, result)
