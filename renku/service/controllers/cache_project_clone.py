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
"""Renku service cache clone project controller."""
from marshmallow import EXCLUDE

from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.controllers.utils.project_clone import user_project_clone
from renku.service.serializers.cache import ProjectCloneContext, ProjectCloneRequest, ProjectCloneResponseRPC
from renku.service.views import result_response


class ProjectCloneCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for cloning a project endpoint."""

    REQUEST_SERIALIZER = ProjectCloneRequest()
    RESPONSE_SERIALIZER = ProjectCloneResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct controller."""
        self.request_data = ProjectCloneCtrl.REQUEST_SERIALIZER.load(request_data)
        self.ctx = ProjectCloneContext().load({**user_data, **self.request_data}, unknown=EXCLUDE)
        super(ProjectCloneCtrl, self).__init__(cache, user_data, self.request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def project_clone(self):
        """Clones a remote project."""
        return user_project_clone(self.user_data, self.ctx)

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(ProjectCloneCtrl.RESPONSE_SERIALIZER, self.project_clone())
