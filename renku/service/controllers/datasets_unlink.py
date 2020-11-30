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
"""Renku service datasets unlink controller."""
from renku.core.commands.dataset import file_unlink
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadWithSyncOperation
from renku.service.serializers.datasets import DatasetUnlinkRequest, DatasetUnlinkResponseRPC
from renku.service.views import result_response


class DatasetsUnlinkCtrl(ServiceCtrl, ReadWithSyncOperation):
    """Controller for datasets unlink endpoint."""

    REQUEST_SERIALIZER = DatasetUnlinkRequest()
    RESPONSE_SERIALIZER = DatasetUnlinkResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets unlink list controller."""
        self.ctx = DatasetsUnlinkCtrl.REQUEST_SERIALIZER.load(request_data)

        self.include = self.ctx.get("include_filter")
        self.exclude = self.ctx.get("exclude_filter")

        if self.ctx.get("commit_message") is None:
            if self.include and self.exclude:
                filters = "-I {0} -X {1}".format(self.include, self.exclude)
            elif not self.include and self.exclude:
                filters = "-X {0}".format(self.exclude)
            else:
                filters = "-I {0}".format(self.include)

            self.ctx["commit_message"] = "service: unlink dataset {0} {1}".format(self.ctx["name"], filters)

        super(DatasetsUnlinkCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = (
            file_unlink()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                name=self.ctx["name"],
                include=self.ctx.get("include_filters"),
                exclude=self.ctx.get("exclude_filters"),
                yes=True,
            )
        )

        return result.output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        response = {
            "unlinked": [record.path for record in op_result],
            "remote_branch": remote_branch,
        }

        return result_response(DatasetsUnlinkCtrl.RESPONSE_SERIALIZER, response)
