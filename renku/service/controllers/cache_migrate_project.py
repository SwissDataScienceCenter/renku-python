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
from renku.core.utils.contexts import click_context
from renku.service.cache.models.job import Job
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.service.logger import worker_log
from renku.service.serializers.cache import ProjectMigrateRequest, ProjectMigrateResponseRPC
from renku.service.utils.callback import ServiceCallback
from renku.service.views import result_response


def execute_migration(
    project_path, force_template_update, skip_template_update, skip_docker_update, skip_migrations, commit_message
):
    """Execute project migrations."""
    from renku.core.commands.migrate import migrate_project

    worker_log.debug(f"migrating {project_path}")

    communicator = ServiceCallback()

    with click_context(project_path, "execute_migration"):
        result = (
            migrate_project()
            .with_commit(message=commit_message)
            .with_communicator(communicator)
            .build()
            .execute(
                force_template_update=force_template_update,
                skip_template_update=skip_template_update,
                skip_docker_update=skip_docker_update,
                skip_migrations=skip_migrations,
            )
        )

        was_migrated, template_migrated, docker_migrated = result.output

    worker_log.debug(f"migration finished - was_migrated={was_migrated}")
    return communicator.messages, was_migrated, template_migrated, docker_migrated


class MigrateProjectCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for migrating project endpoint."""

    REQUEST_SERIALIZER = ProjectMigrateRequest()
    RESPONSE_SERIALIZER = ProjectMigrateResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False, skip_lock=False):
        """Construct controller."""
        self.ctx = MigrateProjectCtrl.REQUEST_SERIALIZER.load(request_data)

        self.force_template_update = self.ctx.get("force_template_update", False)
        self.skip_template_update = self.ctx.get("skip_template_update", False)
        self.skip_docker_update = self.ctx.get("skip_docker_update", False)
        self.skip_migrations = self.ctx.get("skip_migrations", False)
        self.commit_message = self.ctx.get("commit_message", None)

        super(MigrateProjectCtrl, self).__init__(
            cache, user_data, request_data, migrate_project=migrate_project, skip_lock=skip_lock
        )

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        messages, was_migrated, template_migrated, docker_migrated = execute_migration(
            self.project_path,
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
        result = self.execute_op()

        if isinstance(result, Job):
            return result_response(MigrateProjectCtrl.JOB_RESPONSE_SERIALIZER, result)

        return result_response(MigrateProjectCtrl.RESPONSE_SERIALIZER, result)
