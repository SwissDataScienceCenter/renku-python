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
"""Renku service datasets import controller."""
import os

from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadWithSyncOperation
from renku.service.jobs.contexts import enqueue_retry
from renku.service.jobs.datasets import dataset_import
from renku.service.jobs.queues import DATASETS_JOB_QUEUE
from renku.service.serializers.datasets import DatasetImportRequest, DatasetImportResponseRPC
from renku.service.views import result_response


class DatasetsImportCtrl(ServiceCtrl, ReadWithSyncOperation):
    """Controller for datasets import endpoint."""

    REQUEST_SERIALIZER = DatasetImportRequest()
    RESPONSE_SERIALIZER = DatasetImportResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets import controller."""
        self.ctx = DatasetsImportCtrl.REQUEST_SERIALIZER.load(request_data)

        if self.ctx.get("commit_message") is None:
            self.ctx["commit_message"] = "service: dataset import of {0}".format(self.ctx["dataset_uri"])

        super(DatasetsImportCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        job = self.cache.make_job(
            self.user,
            # NOTE: To support operation to be execute on remote project, this behaviour should be updated.
            project=self.ctx["project_id"],
            job_data={"renku_op": "dataset_import", "client_extras": self.ctx.get("client_extras")},
        )

        with enqueue_retry(DATASETS_JOB_QUEUE) as queue:
            queue.enqueue(
                dataset_import,
                self.user_data,
                job.job_id,
                self.ctx["project_id"],
                self.ctx["dataset_uri"],
                name=self.ctx.get("name"),
                extract=self.ctx.get("extract", False),
                job_timeout=int(os.getenv("WORKER_DATASET_JOBS_TIMEOUT", 1800)),
                result_ttl=int(os.getenv("WORKER_DATASET_JOBS_RESULT_TTL", 500)),
            )

        return job

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(DatasetsImportCtrl.RESPONSE_SERIALIZER, self.execute_op())
