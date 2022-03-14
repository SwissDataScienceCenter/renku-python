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
"""Renku service datasets edit controller."""
from renku.core.commands.dataset import edit_dataset_command
from renku.core.management.dataset.request_model import ImageRequestModel
from renku.core.utils.metadata import construct_creators
from renku.service.cache.models.job import Job
from renku.service.config import CACHE_UPLOADS_PATH, MESSAGE_PREFIX
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.service.controllers.utils.datasets import set_url_for_uploaded_images
from renku.service.serializers.datasets import DatasetEditRequest, DatasetEditResponseRPC
from renku.service.views import result_response


class DatasetsEditCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for datasets edit endpoint."""

    REQUEST_SERIALIZER = DatasetEditRequest()
    RESPONSE_SERIALIZER = DatasetEditResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a datasets edit list controller."""
        self.ctx = DatasetsEditCtrl.REQUEST_SERIALIZER.load(request_data)
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} dataset edit {self.ctx['name']}"

        super(DatasetsEditCtrl, self).__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        warnings = []

        images = self.ctx.get("images")
        if images:
            user_cache_dir = CACHE_UPLOADS_PATH / self.user.user_id

            set_url_for_uploaded_images(images=images, cache=self.cache, user=self.user)

            images = [
                ImageRequestModel(
                    content_url=img["content_url"],
                    position=img["position"],
                    mirror_locally=img.get("mirror_locally", False),
                    safe_image_paths=[user_cache_dir],
                )
                for img in images
            ]

        creators = self.ctx.get("creators")
        if creators:
            creators, warnings = construct_creators(creators)

        result = (
            edit_dataset_command()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                self.ctx["name"],
                self.ctx.get("title"),
                self.ctx.get("description"),
                creators,
                keywords=self.ctx.get("keywords"),
                images=images,
                custom_metadata=self.ctx.get("custom_metadata"),
            )
        )

        edited = result.output
        return edited, warnings

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        if isinstance(op_result, Job):
            return result_response(DatasetsEditCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        edited, warnings = op_result

        if "creators" in edited:
            edited["creators"] = self.ctx.get("creators")

        response = {
            "edited": edited,
            "warnings": warnings,
            "remote_branch": remote_branch,
        }

        return result_response(DatasetsEditCtrl.RESPONSE_SERIALIZER, response)
