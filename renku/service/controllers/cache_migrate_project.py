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
"""Renku service migrate project controller."""
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadWithSyncOperation
from renku.service.jobs.contexts import enqueue_retry
from renku.service.jobs.project import execute_migration, migrate_job
from renku.service.jobs.queues import MIGRATIONS_JOB_QUEUE
from renku.service.serializers.cache import (
    ProjectMigrateAsyncResponseRPC,
    ProjectMigrateRequest,
    ProjectMigrateResponseRPC,
)
from renku.service.views import result_response


class MigrateProjectCtrl(ServiceCtrl, ReadWithSyncOperation):
    """Controller for migrating project endpoint."""

    REQUEST_SERIALIZER = ProjectMigrateRequest()
    RESPONSE_SERIALIZER = ProjectMigrateResponseRPC()
    JOB_RESPONSE_SERIALIZER = ProjectMigrateAsyncResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct controller."""
        self.ctx = MigrateProjectCtrl.REQUEST_SERIALIZER.load(request_data)

        self.force_template_update = self.ctx.get("force_template_update", False)
        self.skip_template_update = self.ctx.get("skip_template_update", False)
        self.skip_docker_update = self.ctx.get("skip_docker_update", False)
        self.skip_migrations = self.ctx.get("skip_migrations", False)
        self.commit_message = self.ctx.get("commit_message", None)
        super(MigrateProjectCtrl, self).__init__(cache, user_data, request_data)

        self.project = self.cache.get_project(self.user, self.ctx["project_id"])

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        messages, was_migrated, template_migrated, docker_migrated = execute_migration(
            self.project,
            self.force_template_update,
            self.skip_template_update,
            self.skip_docker_update,
            self.skip_migrations,
            self.commit_message,
        )
        response = {
            "messages": messages,
            "was_migrated": was_migrated,
            "template_migrated": template_migrated,
            "docker_migrated": docker_migrated,
        }

        if was_migrated or template_migrated or docker_migrated:
            response["remote_branch"] = self.sync()

        return response

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        if self.ctx.get("is_delayed", False):
            job = self.cache.make_job(
                self.user,
                project=self.project,
                job_data={"renku_op": "migrate_job", "client_extras": self.ctx.get("client_extras")},
            )

            with enqueue_retry(MIGRATIONS_JOB_QUEUE) as queue:
                queue.enqueue(
                    migrate_job, self.user_data, self.project.project_id, job.job_id, self.commit_message,
                )

            return result_response(MigrateProjectCtrl.JOB_RESPONSE_SERIALIZER, job)

        return result_response(MigrateProjectCtrl.RESPONSE_SERIALIZER, self.execute_op())
