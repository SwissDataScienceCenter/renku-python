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
"""Renku service datasets add controller."""
import json
import os
from pathlib import Path

from renku.command.dataset import add_to_dataset_command
from renku.core.errors import RenkuException
from renku.ui.service.cache.models.job import Job
from renku.ui.service.config import MESSAGE_PREFIX
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.jobs.contexts import enqueue_retry
from renku.ui.service.jobs.datasets import dataset_add_remote_file
from renku.ui.service.jobs.queues import DATASETS_JOB_QUEUE
from renku.ui.service.serializers.datasets import DatasetAddRequest, DatasetAddResponseRPC
from renku.ui.service.views import result_response


class DatasetsAddFileCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for datasets add endpoint."""

    REQUEST_SERIALIZER = DatasetAddRequest()
    RESPONSE_SERIALIZER = DatasetAddResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a datasets add controller."""
        self.ctx = DatasetsAddFileCtrl.REQUEST_SERIALIZER.load(request_data)
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} dataset add {self.ctx['name']}"

        super(DatasetsAddFileCtrl, self).__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def prepare_paths(self):
        """Prepare local paths which can be added in same transaction."""
        local_paths, enqueued_paths = [], []

        for _file in self.ctx["files"]:
            local_path = None

            if "file_url" in _file:
                commit_message = self.ctx["commit_message"]

                job = self.cache.make_job(
                    self.user,
                    # NOTE: To support operation to be executed on remote project, this behaviour should be updated.
                    project=self.ctx["project_id"],
                    job_data={"renku_op": "dataset_add_remote_file", "client_extras": self.ctx.get("client_extras")},
                )
                _file["job_id"] = job.job_id

                with enqueue_retry(DATASETS_JOB_QUEUE) as queue:
                    queue.enqueue(
                        dataset_add_remote_file,
                        self.user_data,
                        job.job_id,
                        # NOTE: To support operation to be executed on remote project, this behaviour should be updated.
                        self.ctx["project_id"],
                        self.ctx["create_dataset"],
                        commit_message,
                        self.ctx["name"],
                        _file["file_url"],
                        job_timeout=int(os.getenv("WORKER_DATASET_JOBS_TIMEOUT", 1800)),
                        result_ttl=int(os.getenv("WORKER_DATASET_JOBS_RESULT_TTL", 500)),
                    )
                    enqueued_paths.append(_file["file_url"])

                continue

            if "file_id" in _file:
                file = self.cache.get_file(self.user, _file["file_id"])
                local_path = file.abs_path
            elif "file_path" in _file:
                if self.project_path is None:
                    raise RenkuException("project_path not set.")

                local_path = self.project_path / Path(_file["file_path"])
            if not local_path or not local_path.exists():
                raise RenkuException(f"invalid file reference: {json.dumps(_file)}")

            self.ctx["commit_message"] += f" {local_path.name}"
            local_paths.append(str(local_path))

        return local_paths, enqueued_paths

    def renku_op(self):
        """Renku operation for the controller."""
        local_paths, enqueued_paths = self.prepare_paths()

        if not local_paths and not enqueued_paths:
            raise RenkuException("no files processed")

        if local_paths:
            add_to_dataset_command().with_commit_message(self.ctx["commit_message"]).build().execute(
                dataset_name=self.ctx["name"],
                urls=local_paths,
                create=self.ctx["create_dataset"],
                force=self.ctx["force"],
                copy=True,
            )

        return local_paths, enqueued_paths

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        if isinstance(op_result, Job):
            return result_response(DatasetsAddFileCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        local_paths, enqueued_paths = op_result
        response = {
            **self.ctx,
            **{"local_paths": local_paths, "enqueued_paths": enqueued_paths, "remote_branch": remote_branch},
        }

        return result_response(DatasetsAddFileCtrl.RESPONSE_SERIALIZER, response)
