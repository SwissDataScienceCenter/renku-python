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
"""Renku service migrations check controller."""

import tempfile
from pathlib import Path

from renku.command.migrate import migrations_check
from renku.core.errors import AuthenticationError, MinimumVersionError, ProjectNotFound, RenkuException
from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION
from renku.core.util.contexts import renku_project_context
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.interfaces.git_api_provider import IGitAPIProvider
from renku.ui.service.serializers.cache import ProjectMigrationCheckRequest, ProjectMigrationCheckResponseRPC
from renku.ui.service.views import result_response
from renku.version import __version__


class MigrationsCheckCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for migrations check endpoint."""

    REQUEST_SERIALIZER = ProjectMigrationCheckRequest()
    RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC()

    def __init__(self, cache, user_data, request_data, git_api_provider: IGitAPIProvider):
        """Construct migration check controller."""
        self.ctx = MigrationsCheckCtrl.REQUEST_SERIALIZER.load(request_data)
        self.git_api_provider = git_api_provider
        super(MigrationsCheckCtrl, self).__init__(cache, user_data, request_data)

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
                [".renku/metadata/root", ".renku/metadata/project", ".renku/metadata.yml", "Dockerfile"],
                tempdir_path,
                remote=self.ctx["git_url"],
                ref=self.request_data.get("ref", None),
                token=self.user_data.get("token", None),
            )
            with renku_project_context(tempdir_path):
                return self.renku_op()

    def renku_op(self):
        """Renku operation for the controller."""
        try:
            return migrations_check().build().execute().output
        except MinimumVersionError as e:
            return {
                "project_supported": False,
                "core_renku_version": e.current_version,
                "project_renku_version": f">={e.minimum_version}",
                "core_compatibility_status": {
                    "migration_required": False,
                    "project_metadata_version": f">={SUPPORTED_PROJECT_VERSION}",
                    "current_metadata_version": SUPPORTED_PROJECT_VERSION,
                },
                "dockerfile_renku_status": {
                    "dockerfile_renku_version": "unknown",
                    "latest_renku_version": __version__,
                    "newer_renku_available": False,
                    "automated_dockerfile_update": False,
                },
                "template_status": {
                    "automated_template_update": False,
                    "newer_template_available": False,
                    "template_source": "unknown",
                    "template_ref": "unknown",
                    "template_id": "unknown",
                    "project_template_version": "unknown",
                    "latest_template_version": "unknown",
                },
            }

    def to_response(self):
        """Execute controller flow and serialize to service response."""
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

        return result_response(self.RESPONSE_SERIALIZER, result)
