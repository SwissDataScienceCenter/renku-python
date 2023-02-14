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
"""Renku service cache project related models."""
import os
import shutil
from datetime import datetime
from typing import Optional

import portalocker
from walrus import BooleanField, DateTimeField, IntegerField, Model, TextField

from renku.ui.service.cache.base import BaseCache
from renku.ui.service.config import CACHE_PROJECTS_PATH

MAX_CONCURRENT_PROJECT_REQUESTS = 10
LOCK_TIMEOUT = 15


class Project(Model):
    """User project object."""

    __database__ = BaseCache.model_db
    __namespace__ = BaseCache.namespace

    created_at = DateTimeField()
    last_fetched_at = DateTimeField()

    project_id = TextField(primary_key=True, index=True)
    user_id = TextField(index=True)

    clone_depth = IntegerField()
    git_url = TextField(index=True)

    name = TextField()
    slug = TextField()
    fullname = TextField()
    description = TextField()
    email = TextField()
    owner = TextField()
    token = TextField()
    initialized = BooleanField()

    @property
    def abs_path(self):
        """Full path of cached project."""
        return CACHE_PROJECTS_PATH / self.user_id / self.owner / self.slug

    def read_lock(self, timeout: Optional[float] = None):
        """Shared read lock on the project."""
        timeout = timeout if timeout is not None else LOCK_TIMEOUT
        return portalocker.Lock(
            f"{self.abs_path}.lock", flags=portalocker.LOCK_SH | portalocker.LOCK_NB, timeout=timeout
        )

    def write_lock(self):
        """Exclusive write lock on the project."""
        return portalocker.Lock(f"{self.abs_path}.lock", flags=portalocker.LOCK_EX, timeout=LOCK_TIMEOUT)

    def concurrency_lock(self):
        """Lock to limit concurrent operations on a project.

        This serves as a "leaky bucket" type approach to prevent starvation with multiple
        concurrent requests.
        """
        return portalocker.BoundedSemaphore(
            MAX_CONCURRENT_PROJECT_REQUESTS, name=f"{self.name}_bounded_semaphore", directory=str(self.abs_path.parent)
        )

    @property
    def age(self):
        """Returns project's age in seconds."""
        # NOTE: `created_at` field is aligned to UTC timezone.
        return int((datetime.utcnow() - self.created_at).total_seconds())

    @property
    def fetch_age(self):
        """Returns project's fetch age in seconds."""
        return int((datetime.utcnow() - self.last_fetched_at).total_seconds())

    def exists(self):
        """Ensure a project exists on file system."""
        return self.abs_path.exists()

    def ttl_expired(self, ttl=None):
        """Check if project time to live has expired."""
        if not self.created_at:
            # If record does not contain created_at,
            # it means its an old record, and
            # we should mark it for deletion.
            return True

        # NOTE: time to live measured in seconds
        ttl = ttl or int(os.getenv("RENKU_SVC_CLEANUP_TTL_PROJECTS", 1800))
        return self.age >= ttl

    def purge(self):
        """Removes project from file system and cache."""
        shutil.rmtree(str(self.abs_path))
        self.delete()

    def is_locked(self, jobs):
        """Check if file locked by given jobs."""
        return bool(next((job for job in jobs if self.project_id in job.locked), False))
