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
from renku.core.util.contexts import renku_project_context
from renku.ui.service.cache.models.job import Job
from renku.ui.service.config import MESSAGE_PREFIX, PROJECT_CLONE_NO_DEPTH
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.logger import worker_log
from renku.ui.service.serializers.cache import ProjectMigrateRequest, ProjectMigrateResponseRPC
from renku.ui.service.utils.callback import ServiceCallback
from renku.ui.service.views import result_response
from renku.version import __version__


def execute_migration(
    project_path, force_template_update, skip_template_update, skip_docker_update, skip_migrations, commit_message
):
    """Execute project migrations."""
    from renku.command.migrate import migrate_project_command

    worker_log.debug(f"migrating {project_path}")

    communicator = ServiceCallback()

    with renku_project_context(project_path):
        result = (
            migrate_project_command()
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
    return (
        was_migrated,
        template_migrated,
        docker_migrated,
        communicator.messages,
        communicator.warnings,
        communicator.errors,
    )


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
        self.commit_message = f"{MESSAGE_PREFIX} migrate to latest version for renku {__version__}"

        super(MigrateProjectCtrl, self).__init__(
            cache,
            user_data,
            request_data,
            migrate_project=migrate_project,
            skip_lock=skip_lock,
            clone_depth=PROJECT_CLONE_NO_DEPTH,
        )

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        return execute_migration(
            self.project_path,
            self.force_template_update,
            self.skip_template_update,
            self.skip_docker_update,
            self.skip_migrations,
            self.commit_message,
        )

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()
        if isinstance(op_result, Job):
            return result_response(MigrateProjectCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        was_migrated, template_migrated, docker_migrated, messages, warnings, errors = op_result

        response = {
            "messages": messages,
            "warnings": warnings,
            "errors": errors,
            "was_migrated": was_migrated,
            "template_migrated": template_migrated,
            "docker_migrated": docker_migrated,
            "remote_branch": remote_branch,
        }

        return result_response(self.RESPONSE_SERIALIZER, response)
