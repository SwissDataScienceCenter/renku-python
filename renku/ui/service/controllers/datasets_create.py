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
from renku.command.dataset import create_dataset_command
from renku.core.dataset.request_model import ImageRequestModel
from renku.core.util.metadata import construct_creators
from renku.ui.service.cache.models.job import Job
from renku.ui.service.config import CACHE_UPLOADS_PATH, MESSAGE_PREFIX
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.controllers.utils.datasets import set_url_for_uploaded_images
from renku.ui.service.serializers.datasets import DatasetCreateRequest, DatasetCreateResponseRPC
from renku.ui.service.views import result_response


class DatasetsCreateCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for datasets create endpoint."""

    REQUEST_SERIALIZER = DatasetCreateRequest()
    RESPONSE_SERIALIZER = DatasetCreateResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct a datasets create controller."""
        self.ctx = DatasetsCreateCtrl.REQUEST_SERIALIZER.load(request_data)
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} dataset create {self.ctx['name']}"

        super(DatasetsCreateCtrl, self).__init__(cache, user_data, request_data, migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
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
            creators, _ = construct_creators(creators)

        return (
            create_dataset_command()
            .with_commit_message(self.ctx["commit_message"])
            .build()
            .execute(
                name=self.ctx["name"],
                title=self.ctx.get("title"),
                creators=creators,
                description=self.ctx.get("description"),
                keywords=self.ctx.get("keywords"),
                images=images,
                custom_metadata=self.ctx.get("custom_metadata"),
                datadir=self.ctx.get("data_directory"),
            )
        )

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        if isinstance(op_result, Job):
            return result_response(DatasetsCreateCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        op_result = self.ctx
        op_result["remote_branch"] = remote_branch

        return result_response(DatasetsCreateCtrl.RESPONSE_SERIALIZER, op_result)
