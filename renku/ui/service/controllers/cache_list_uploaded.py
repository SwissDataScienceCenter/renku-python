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
"""Renku service cache list uploaded files controller."""
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.cache import FileListResponseRPC
from renku.ui.service.views import result_response


class ListUploadedFilesCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for listing uploaded files endpoint."""

    RESPONSE_SERIALIZER = FileListResponseRPC()

    def __init__(self, cache, user_data):
        """Construct list uploaded files controller."""
        self.ctx = {}
        super(ListUploadedFilesCtrl, self).__init__(cache, user_data, {})

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""

        files = [f for f in self.cache.get_files(self.user) if f.exists()]
        return {"files": sorted(files, key=lambda rec: (rec.is_dir, rec.relative_path))}

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(ListUploadedFilesCtrl.RESPONSE_SERIALIZER, self.renku_op())
