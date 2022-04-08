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
from renku.command.dataset import file_unlink_command
from renku.ui.service.cache.models.job import Job
from renku.ui.service.config import MESSAGE_PREFIX
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.serializers.datasets import DatasetUnlinkRequest, DatasetUnlinkResponseRPC
from renku.ui.service.views import result_response


class DatasetsUnlinkCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for datasets unlink endpoint."""

    REQUEST_SERIALIZER = DatasetUnlinkRequest()
    RESPONSE_SERIALIZER = DatasetUnlinkResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a datasets unlink list controller."""
        self.ctx = DatasetsUnlinkCtrl.REQUEST_SERIALIZER.load(request_data)

        self.include = self.ctx.get("include_filter")
        self.exclude = self.ctx.get("exclude_filter")

        if self.include and self.exclude:
            filters = f"-I {self.include} -X {self.exclude}"
        elif not self.include and self.exclude:
            filters = f"-X {self.exclude}"
        else:
            filters = f"-I {self.include}"
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} unlink dataset {self.ctx['name']} {filters}"

        super(DatasetsUnlinkCtrl, self).__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = (
            file_unlink_command()
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

        if isinstance(op_result, Job):
            return result_response(DatasetsUnlinkCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        response = {
            "unlinked": [record.entity.path for record in op_result],
            "remote_branch": remote_branch,
        }

        return result_response(DatasetsUnlinkCtrl.RESPONSE_SERIALIZER, response)
