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
"""Renku service datasets files controller."""
from renku.core.commands.dataset import list_files
from renku.core.utils.contexts import chdir
from renku.service.controllers.remote_project import RemoteProject
from renku.service.serializers.datasets import DatasetFilesListRequest, DatasetFilesListResponseRPC
from renku.service.views import result_response


class DatasetsFilesListCtrl:
    """Controller for datasets files list endpoint."""

    REQUEST_SERIALIZER = DatasetFilesListRequest()
    RESPONSE_SERIALIZER = DatasetFilesListResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets files list controller."""
        self.ctx = DatasetsFilesListCtrl.REQUEST_SERIALIZER.load(request_data)
        self.user = cache.ensure_user(user_data)

        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

    def renku_op(self):
        """Renku operation for the controller."""
        return list_files(datasets=[self.ctx["name"]])

    def local(self):
        """Execute renku operation against service cache."""
        project = self.cache.get_project(self.user, self.ctx["project_id"])

        with chdir(project.abs_path):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        project = RemoteProject(self.user_data, self.request_data)

        with project.remote():
            return self.renku_op()

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["files"] = []

        if "project_id" in self.ctx:
            self.ctx["files"] = self.local()

        elif "git_url" in self.ctx:
            self.ctx["files"] = self.remote()

        return result_response(DatasetsFilesListCtrl.RESPONSE_SERIALIZER, self.ctx)
