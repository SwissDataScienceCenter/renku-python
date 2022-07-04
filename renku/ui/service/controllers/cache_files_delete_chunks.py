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
"""Renku service cache delete uploaded chunks."""
import shutil

from renku.ui.service.config import CACHE_UPLOADS_PATH
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.errors import IntermittentFileNotExistsError
from renku.ui.service.serializers.cache import FileChunksDeleteRequest, FileChunksDeleteResponseRPC
from renku.ui.service.views import result_response


class DeleteFileChunksCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for deleting uploaded chunks."""

    REQUEST_SERIALIZER = FileChunksDeleteRequest()
    RESPONSE_SERIALIZER = FileChunksDeleteResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct list uploaded files controller."""
        self.ctx = DeleteFileChunksCtrl.REQUEST_SERIALIZER.load(request_data)
        super(DeleteFileChunksCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def delete_chunks(self):
        """Delete chunks from a chunked upload."""
        chunked_id = self.context["chunked_id"]

        user_cache_dir = CACHE_UPLOADS_PATH / self.user.user_id
        chunks_dir = user_cache_dir / chunked_id

        if not chunks_dir.exists():
            raise IntermittentFileNotExistsError(file_name=chunked_id)

        shutil.rmtree(chunks_dir)

        self.cache.invalidate_chunks(self.user, chunked_id)

        return f"Deleted chunks for {chunked_id}"

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(DeleteFileChunksCtrl.RESPONSE_SERIALIZER, self.delete_chunks())
