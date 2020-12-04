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
"""Renku service datasets files controller."""

from renku.core.commands.dataset import list_files
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.serializers.datasets import DatasetFilesListRequest, DatasetFilesListResponseRPC
from renku.service.views import result_response


class DatasetsFilesListCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for datasets files list endpoint."""

    REQUEST_SERIALIZER = DatasetFilesListRequest()
    RESPONSE_SERIALIZER = DatasetFilesListResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets files list controller."""
        self.ctx = DatasetsFilesListCtrl.REQUEST_SERIALIZER.load(request_data)

        super(DatasetsFilesListCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = list_files().build().execute(datasets=[self.ctx["name"]])
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["files"] = self.execute_op()
        return result_response(DatasetsFilesListCtrl.RESPONSE_SERIALIZER, self.ctx)
