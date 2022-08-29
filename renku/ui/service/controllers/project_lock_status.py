# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Renku service project lock status controller."""

import portalocker

from renku.core import errors
from renku.ui.service.cache.models.project import Project
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.errors import IntermittentProjectIdError
from renku.ui.service.serializers.project import ProjectLockStatusRequest, ProjectLockStatusResponseRPC
from renku.ui.service.views import result_response


class ProjectLockStatusCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for project lock status endpoint."""

    REQUEST_SERIALIZER = ProjectLockStatusRequest()
    RESPONSE_SERIALIZER = ProjectLockStatusResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a project edit controller."""
        self.ctx = self.REQUEST_SERIALIZER.load(request_data)

        super().__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def get_lock_status(self) -> bool:
        """Return True if a project is write-locked."""
        if "project_id" in self.context:
            try:
                project = self.cache.get_project(self.user, self.context["project_id"])
            except IntermittentProjectIdError:
                return False
        elif "git_url" in self.context and "user_id" in self.user_data:
            try:
                project = Project.get(
                    (Project.user_id == self.user_data["user_id"]) & (Project.git_url == self.context["git_url"])
                )
            except ValueError:
                return False
        else:
            raise errors.RenkuException("context does not contain `project_id` or `git_url` or missing `user_id`")

        try:
            with project.read_lock(timeout=self.ctx["timeout"]):
                return False
        except (portalocker.LockException, portalocker.AlreadyLocked):
            return True

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        is_locked = self.get_lock_status()
        return result_response(self.RESPONSE_SERIALIZER, data={"locked": is_locked})
