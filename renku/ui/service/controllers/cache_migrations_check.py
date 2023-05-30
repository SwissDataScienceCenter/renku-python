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
"""Renku service migrations check controller."""

import tempfile
from dataclasses import asdict
from pathlib import Path

from renku.command.doctor import doctor_check_command
from renku.command.migrate import MigrationCheckResult, migrations_check
from renku.core.errors import AuthenticationError, MinimumVersionError, ProjectNotFound, RenkuException
from renku.core.util.contexts import renku_project_context
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.interfaces.git_api_provider import IGitAPIProvider
from renku.ui.service.serializers.cache import ProjectMigrationCheckRequest, ProjectMigrationCheckResponseRPC
from renku.ui.service.views import result_response


class MigrationsCheckCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for migrations check endpoint."""

    REQUEST_SERIALIZER = ProjectMigrationCheckRequest()
    RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC()

    def __init__(self, cache, user_data, request_data, git_api_provider: IGitAPIProvider):
        """Construct migration check controller."""
        self.ctx = MigrationsCheckCtrl.REQUEST_SERIALIZER.load(request_data)
        self.git_api_provider = git_api_provider
        super().__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def _fast_op_without_cache(self):
        """Execute renku_op with only necessary files, without cloning the whole repo."""
        if "git_url" not in self.context:
            raise RenkuException("context does not contain `project_id` or `git_url`")

        with tempfile.TemporaryDirectory() as tempdir:
            tempdir_path = Path(tempdir)
            self.git_api_provider.download_files_from_api(
                files=[
                    "Dockerfile",
                ],
                folders=[".renku"],
                target_folder=tempdir_path,
                remote=self.ctx["git_url"],
                branch=self.request_data.get("branch", None),
                token=self.user_data.get("token", None),
            )
            with renku_project_context(tempdir_path):
                return self.renku_op()

    def renku_op(self):
        """Renku operation for the controller."""
        try:
            migrations_check_result = migrations_check().build().execute().output
            doctor_result = doctor_check_command(with_fix=False).build().execute(fix=False, force=False).output
            migrations_check_result.core_compatibility_status.fixes_available = doctor_result[1]
            migrations_check_result.core_compatibility_status.issues_found = doctor_result[2]
            return migrations_check_result
        except MinimumVersionError as e:
            return MigrationCheckResult.from_minimum_version_error(e)

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        from renku.ui.service.views.error_handlers import pretty_print_error

        if "project_id" in self.context:
            result = self.execute_op()
        else:
            # NOTE: use quick flow but fallback to regular flow in case of unexpected exceptions
            try:
                result = self._fast_op_without_cache()
            except (AuthenticationError, ProjectNotFound):
                raise
            except BaseException:
                result = self.execute_op()

        result_dict = asdict(result)

        # NOTE: Pretty-print errors for the UI
        if isinstance(result.template_status, Exception):
            result_dict["template_status"] = pretty_print_error(result.template_status)

        if isinstance(result.dockerfile_renku_status, Exception):
            result_dict["dockerfile_renku_status"] = pretty_print_error(result.dockerfile_renku_status)

        if isinstance(result.core_compatibility_status, Exception):
            result_dict["core_compatibility_status"] = pretty_print_error(result.core_compatibility_status)

        return result_response(self.RESPONSE_SERIALIZER, result_dict)
