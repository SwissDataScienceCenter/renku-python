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
"""Renku service cache list cached projects controller."""
import itertools

from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.serializers.cache import ProjectListResponseRPC
from renku.service.views import result_response


class ListProjectsCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for listing cached projects endpoint."""

    RESPONSE_SERIALIZER = ProjectListResponseRPC()

    def __init__(self, cache, user_data):
        """Construct controller."""
        self.ctx = {}
        super(ListProjectsCtrl, self).__init__(cache, user_data, {})

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def list_projects(self):
        """List locally cache projects."""
        projects = [project for project in self.cache.get_projects(self.user) if project.abs_path.exists()]

        result = {
            "projects": [
                max(g, key=lambda p: p.created_at) for _, g in itertools.groupby(projects, lambda p: p.git_url)
            ]
        }

        return result

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(ListProjectsCtrl.RESPONSE_SERIALIZER, self.list_projects())
