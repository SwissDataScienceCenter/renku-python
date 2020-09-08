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
"""Renku service controller mixin."""

from abc import abstractmethod

from renku.core.utils.contexts import chdir
from renku.service.controllers.utils.remote_project import RemoteProject


class ReadOperationMixin:
    """Read operation mixin."""

    def __init__(self, cache, user_data, request_data):
        """Read operation mixin for controllers."""
        self.user = cache.ensure_user(user_data)

        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

    @property
    @abstractmethod
    def context(self):
        """Operation context."""
        raise NotImplementedError

    @abstractmethod
    def renku_op(self):
        """Implements renku operation for the controller."""
        raise NotImplementedError

    def local(self):
        """Execute renku operation against service cache."""
        project = self.cache.get_project(self.user, self.context["project_id"])

        with chdir(project.abs_path):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        project = RemoteProject(self.user_data, self.request_data)

        with project.remote():
            return self.renku_op()
