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
"""Renku service user cache management."""
from renku.ui.service.cache.base import BaseCache
from renku.ui.service.cache.models.user import User
from renku.ui.service.cache.serializers.user import UserSchema


class UserManagementCache(BaseCache):
    """User management cache."""

    user_schema = UserSchema()

    def ensure_user(self, user_data):
        """Ensure user data registered in a cache."""
        user_obj = self.user_schema.load(user_data)

        try:
            User.get(User.user_id == user_obj.user_id)
        except ValueError:
            user_obj.save()

        return user_obj

    @staticmethod
    def get_user(user_id):
        """Get specific user."""
        try:
            user_obj = User.get(User.user_id == user_id)
        except ValueError:
            return

        return user_obj

    @staticmethod
    def get_users():
        """Get all users."""
        return User.all()
