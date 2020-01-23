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
from renku.service.cache.base import BaseCache


class ProjectManagementCache(BaseCache):
    """Project management cache."""

    PROJECTS_SUFFIX = 'projects'

    def projects_cache_key(self, user):
        """Construct cache key based on user and projects suffix."""
        return '{0}_{1}'.format(user['user_id'], self.PROJECTS_SUFFIX)

    def set_project(self, user, project_id, metadata):
        """Cache project metadata under user hash set."""
        self.set_record(self.projects_cache_key(user), project_id, metadata)

    def get_projects(self, user):
        """Get all user cache projects."""
        return self.get_all_records(self.projects_cache_key(user))

    def get_project(self, user, project_id):
        """Get user cached project."""
        result = self.get_record(self.projects_cache_key(user), project_id)
        return result

    def invalidate_project(self, user, project_id):
        """Remove project record from hash set."""
        self.invalidate_key(self.projects_cache_key(user), project_id)

    def all_projects_iter(self):
        """Iterate over cached projects."""
        return self.scan_iter(
            '*_{0}'.format(ProjectManagementCache.PROJECTS_SUFFIX)
        )
