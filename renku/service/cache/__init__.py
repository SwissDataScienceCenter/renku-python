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
"""Renku service cache management for files."""
from renku.service.cache.files import FileManagementCache
from renku.service.cache.jobs import JobManagementCache
from renku.service.cache.projects import ProjectManagementCache
from renku.service.cache.users import UserManagementCache
from renku.service.config import CACHE_PROJECTS_PATH, CACHE_UPLOADS_PATH


class ServiceCache(FileManagementCache, ProjectManagementCache, JobManagementCache, UserManagementCache):
    """Service cache manager."""

    pass


def make_cache():
    """Create cache structure."""
    sub_dirs = [CACHE_UPLOADS_PATH, CACHE_PROJECTS_PATH]

    for subdir in sub_dirs:
        subdir.mkdir(parents=True, exist_ok=True)

    return ServiceCache()


cache = make_cache()

__all__ = ["cache", "ServiceCache"]
