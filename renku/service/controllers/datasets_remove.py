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
"""Renku service datasets remove controller."""
from renku.core.commands.dataset import remove_dataset
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadWithSyncOperation
from renku.service.serializers.datasets import DatasetRemoveRequest, DatasetRemoveResponseRPC
from renku.service.views import result_response


class DatasetsRemoveCtrl(ServiceCtrl, ReadWithSyncOperation):
    """Controller for datasets remove endpoint."""

    REQUEST_SERIALIZER = DatasetRemoveRequest()
    RESPONSE_SERIALIZER = DatasetRemoveResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets remove controller."""
        self.ctx = DatasetsRemoveCtrl.REQUEST_SERIALIZER.load(request_data)

        if self.ctx.get("commit_message") is None:
            self.ctx["commit_message"] = "service: dataset remove {0}".format(self.ctx["name"])

        super(DatasetsRemoveCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = remove_dataset().with_commit_message(self.ctx["commit_message"]).build().execute(self.ctx["name"])
        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        _, remote_branch = self.execute_and_sync()

        response = self.ctx
        response["remote_branch"] = remote_branch

        return result_response(DatasetsRemoveCtrl.RESPONSE_SERIALIZER, response)
