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
"""Renku service project cache management."""
from marshmallow import EXCLUDE

from renku.ui.service.cache.base import BaseCache
from renku.ui.service.cache.models.project import Project
from renku.ui.service.cache.models.user import User
from renku.ui.service.cache.serializers.project import ProjectSchema
from renku.ui.service.errors import IntermittentProjectIdError


class ProjectManagementCache(BaseCache):
    """Project management cache."""

    project_schema = ProjectSchema()

    def make_project(self, user, project_data, persist=True):
        """Store user project metadata."""
        project_data.update({"user_id": user.user_id})

        project_obj = self.project_schema.load(project_data, unknown=EXCLUDE)

        if persist:
            project_obj.save()

        return project_obj

    @staticmethod
    def get_project(user, project_id):
        """Get user cached project."""
        try:
            record = Project.get((Project.project_id == project_id) & (Project.user_id == user.user_id))
        except ValueError as e:
            raise IntermittentProjectIdError(e)

        if not record.abs_path.exists():
            record.delete()
            raise IntermittentProjectIdError()

        return record

    @staticmethod
    def get_projects(user):
        """Get all user cache projects."""
        return Project.query(Project.user_id == user.user_id)

    @staticmethod
    def invalidate_project(user, project_id):
        """Remove user project record."""
        project_obj = ProjectManagementCache.get_project(user, project_id)

        if project_obj:
            project_obj.purge()

        return project_obj

    def user_projects(self):
        """Iterate through all cached projects."""
        for user in User.all():
            yield user, self.get_projects(user)
