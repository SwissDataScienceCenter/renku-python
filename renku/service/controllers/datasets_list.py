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
"""Renku service datasets list controller."""

from renku.core.commands.dataset import list_datasets
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.serializers.datasets import DatasetListRequest, DatasetListResponseRPC
from renku.service.views import result_response


class DatasetsListCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for datasets list endpoint."""

    REQUEST_SERIALIZER = DatasetListRequest()
    RESPONSE_SERIALIZER = DatasetListResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets list controller."""
        self.ctx = DatasetsListCtrl.REQUEST_SERIALIZER.load(request_data)
        super(DatasetsListCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = list_datasets().build().execute()
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["datasets"] = self.execute_op()
        return result_response(DatasetsListCtrl.RESPONSE_SERIALIZER, self.ctx)
