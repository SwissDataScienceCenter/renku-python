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
from renku.service.cache.models.file import File
from renku.service.cache.models.user import User
from renku.service.cache.serializers.file import FileSchema


class FileManagementCache(BaseCache):
    """File management cache."""

    file_schema = FileSchema()

    def set_file(self, user, file_data):
        """Cache file metadata."""
        file_data.update({'user_id': user.user_id})

        file_obj = self.file_schema.load(file_data)
        file_obj.save()

        return file_obj

    def set_files(self, user, files):
        """Cache files metadata."""
        return [self.set_file(user, file_) for file_ in files]

    @staticmethod
    def get_file(user, file_id):
        """Get user file."""
        try:
            record = File.get((File.file_id == file_id) &
                              (File.user_id == user.user_id))
        except ValueError:
            return

        return record

    @staticmethod
    def get_files(user):
        """Get all user cached files."""
        return File.query(File.user_id == user.user_id)

    @staticmethod
    def invalidate_file(user, file_id):
        """Remove users file records."""
        file_obj = FileManagementCache.get_file(user, file_id)

        if file_obj:
            file_obj.delete()

        return file_obj

    def user_files(self):
        """Iterate through all cached files."""
        for user in User.all():
            yield user, self.get_files(user)
