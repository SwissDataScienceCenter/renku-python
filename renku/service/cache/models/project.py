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
import time
from datetime import datetime

from walrus import DateTimeField, IntegerField, Model, TextField

from renku.service.cache.base import BaseCache
from renku.service.config import CACHE_PROJECTS_PATH


class Project(Model):
    """User project object."""

    __database__ = BaseCache.model_db

    created_at = DateTimeField()

    project_id = TextField(primary_key=True, index=True)
    user_id = TextField(index=True)

    clone_depth = IntegerField()
    git_url = TextField()

    name = TextField()
    fullname = TextField()
    email = TextField()
    owner = TextField()
    token = TextField()

    @property
    def abs_path(self):
        """Full path of cached project."""
        return CACHE_PROJECTS_PATH / self.user_id / self.owner / self.name

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

        ttl = ttl or int(os.getenv("RENKU_SVC_CLEANUP_TTL_PROJECTS", 1800))

        created_at = (self.created_at - datetime.utcfromtimestamp(0)).total_seconds() * 1e3

        age = ((time.time() * 1e3) - created_at) / 1e3
        return self.exists() and age >= ttl

    def purge(self):
        """Removes project from file system and cache."""
        shutil.rmtree(str(self.abs_path))
        self.delete()

    def is_locked(self, jobs):
        """Check if file locked by given jobs."""
        return bool(next((job for job in jobs if self.project_id in job.locked), False))
