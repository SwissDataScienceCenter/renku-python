# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Repository cache interface."""

from abc import ABC
from typing import Optional

from renku.ui.service.cache import ServiceCache
from renku.ui.service.cache.models.project import Project
from renku.ui.service.cache.models.user import User


class IRepositoryCache(ABC):
    """Interface for repository cache manager."""

    def get(
        self, cache: ServiceCache, git_url: str, branch: Optional[str], user: User, shallow: bool = True
    ) -> Project:
        """Get a project from cache (clone if necessary)."""
        raise NotImplementedError()

    def evict(self, project: Project):
        """Evict a project from cache."""
        raise NotImplementedError()

    def evict_expired(self):
        """Evict expired projects from cache."""
        raise NotImplementedError()
