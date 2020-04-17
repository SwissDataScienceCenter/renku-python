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
"""Renku service cache file related models."""
import os
import shutil
import time
from datetime import datetime

from walrus import BooleanField, DateTimeField, IntegerField, Model, TextField

from renku.service.cache.base import BaseCache
from renku.service.config import CACHE_UPLOADS_PATH


class File(Model):
    """User file object."""

    __database__ = BaseCache.model_db

    created_at = DateTimeField()

    file_id = TextField(primary_key=True, index=True)
    user_id = TextField(index=True)

    content_type = TextField()
    file_name = TextField()

    file_size = IntegerField()

    relative_path = TextField()
    is_archive = BooleanField()
    is_dir = BooleanField()
    unpack_archive = BooleanField()

    @property
    def abs_path(self):
        """Full path of cached file."""
        return CACHE_UPLOADS_PATH / self.user_id / self.relative_path

    def exists(self):
        """Ensure a file exists on file system."""
        if self.abs_path.exists():
            self.is_dir = self.abs_path.is_dir()
            return True

        return False

    def ttl_expired(self, ttl=None):
        """Check if file time to live has expired."""
        if not self.created_at:
            # If record does not contain created_at,
            # it means its an old record, and
            # we should mark it for deletion.
            return True

        ttl = ttl or int(os.getenv('RENKU_SVC_CLEANUP_TTL_FILES', 1800))

        created_at = (self.created_at -
                      datetime.utcfromtimestamp(0)).total_seconds() * 1e+3

        age = ((time.time() * 1e+3) - created_at) / 1e+3
        return self.exists() and age >= ttl

    def purge(self):
        """Removes file from file system and cache."""
        if self.abs_path.is_file():
            self.abs_path.unlink()

        if self.abs_path.is_dir():
            shutil.rmtree(str(self.abs_path))

        self.delete()

    def is_locked(self, jobs):
        """Check if file locked by given jobs."""
        return bool(
            next((job for job in jobs if self.file_id in job.locked), False)
        )
