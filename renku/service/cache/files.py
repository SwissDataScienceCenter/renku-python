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
"""Renku service files cache management."""
from renku.service.cache.base import BaseCache


class FileManagementCache(BaseCache):
    """File management cache."""

    FILES_SUFFIX = 'files'

    def files_cache_key(self, user):
        """Construct cache key based on user and files suffix."""
        return '{0}_{1}'.format(user['user_id'], self.FILES_SUFFIX)

    def set_file(self, user, file_id, metadata):
        """Cache file metadata under user hash set."""
        self.set_record(self.files_cache_key(user), file_id, metadata)

    def set_files(self, user, files):
        """Cache a list of metadata files under user hash set."""
        for file_ in files:
            self.set_file(user, file_['file_id'], file_)

    def get_files(self, user):
        """Get all user cached files."""
        return self.get_all_records(self.files_cache_key(user))

    def get_file(self, user, file_id):
        """Get user cached file."""
        result = self.get_record(self.files_cache_key(user), file_id)
        return result

    def invalidate_file(self, user, file_id):
        """Remove file record from hash set."""
        self.invalidate_key(self.files_cache_key(user), file_id)

    def all_files_iter(self):
        """Iterate over cached files."""
        return self.scan_iter('*_{0}'.format(FileManagementCache.FILES_SUFFIX))
