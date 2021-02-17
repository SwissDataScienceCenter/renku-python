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
"""Renku service datasets create controller."""
from renku.core.commands.dataset import create_dataset
from renku.service.config import CACHE_UPLOADS_PATH
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadWithSyncOperation
from renku.service.serializers.datasets import DatasetCreateRequest, DatasetCreateResponseRPC
from renku.service.views import result_response


class DatasetsCreateCtrl(ServiceCtrl, ReadWithSyncOperation):
    """Controller for datasets create endpoint."""

    REQUEST_SERIALIZER = DatasetCreateRequest()
    RESPONSE_SERIALIZER = DatasetCreateResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets create controller."""
        self.ctx = DatasetsCreateCtrl.REQUEST_SERIALIZER.load(request_data)

        if self.ctx.get("commit_message") is None:
            self.ctx["commit_message"] = "service: dataset create {0}".format(self.ctx["name"])

        super(DatasetsCreateCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def _handle_uploaded_images(self):
        """Handles uploaded or relative dataset images."""
        for img in self.ctx.get("images", []):
            if img.get("file_id"):
                file = self.cache.get_file(self.user, img.pop("file_id"))
                img["content_url"] = str(file.abs_path)

    def renku_op(self):
        """Renku operation for the controller."""
        self._handle_uploaded_images()
        user_cache_dir = CACHE_UPLOADS_PATH / self.user.user_id

        return (
            create_dataset()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                name=self.ctx["name"],
                title=self.ctx.get("title"),
                creators=self.ctx.get("creators"),
                description=self.ctx.get("description"),
                keywords=self.ctx.get("keywords"),
                images=self.ctx.get("images"),
                safe_image_paths=[user_cache_dir],
            )
        )

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        _, remote_branch = self.execute_and_sync()

        response = self.ctx
        response["remote_branch"] = remote_branch

        return result_response(DatasetsCreateCtrl.RESPONSE_SERIALIZER, response)
