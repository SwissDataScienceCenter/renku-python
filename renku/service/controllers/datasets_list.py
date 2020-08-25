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
"""Renku service datasets list controller."""
from renku.core.commands.dataset import list_datasets
from renku.core.utils.contexts import chdir
from renku.service.controllers.remote_project import RemoteProject
from renku.service.serializers.datasets import DatasetListRequest, DatasetListResponseRPC
from renku.service.views import result_response


class DatasetsListCtrl:
    """Controller for datasets list endpoint."""

    REQUEST_SERIALIZER = DatasetListRequest()
    RESPONSE_SERIALIZER = DatasetListResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets list controller."""
        self.ctx = DatasetsListCtrl.REQUEST_SERIALIZER.load(request_data)
        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

    def renku_op(self):
        """Renku operation for the controller."""
        return list_datasets()

    def local(self):
        """Execute renku operation against service cache."""
        project = self.cache.get_project(self.cache.ensure_user(self.user_data), self.ctx["project_id"])

        with chdir(project.abs_path):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        project = RemoteProject(self.user_data, self.request_data)

        with project.remote():
            return self.renku_op()

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["datasets"] = []

        if "project_id" in self.ctx:
            self.ctx["datasets"] = self.local()

        elif "git_url" in self.ctx:
            self.ctx["datasets"] = self.remote()

        return result_response(DatasetsListCtrl.RESPONSE_SERIALIZER, self.ctx)
