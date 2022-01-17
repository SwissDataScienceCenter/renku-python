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

from renku.core.commands.migrate import migrations_check
from renku.core.errors import RenkuException
from renku.core.utils.contexts import click_context
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import RenkuOperationMixin
from renku.service.interfaces.git_api_provider import IGitAPIProvider
from renku.service.serializers.cache import ProjectMigrationCheckRequest, ProjectMigrationCheckResponseRPC
from renku.service.views import result_response


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
            tempdir = Path(tempdir)

            self.git_api_provider.download_files_from_api(
                [".renku/metadata/root", ".renku/metadata/project", ".renku/metadata.yml", "Dockerfile"],
                tempdir,
                remote=self.ctx["git_url"],
                ref=self.request_data.get("ref", None),
                token=self.user_data.get("token", None),
            )
            with click_context(tempdir, "renku_op"):
                return self.renku_op()

    def renku_op(self):
        """Renku operation for the controller."""
        return migrations_check().build().execute().output

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(self.RESPONSE_SERIALIZER, self.execute_op())

        # TODO: Enable this optimization. See https://github.com/SwissDataScienceCenter/renku-python/issues/2546
        # if "project_id" in self.context:
        #     # use regular flow using cache
        #     return result_response(self.RESPONSE_SERIALIZER, self.execute_op())
        #
        # return result_response(self.RESPONSE_SERIALIZER, self._fast_op_without_cache())
