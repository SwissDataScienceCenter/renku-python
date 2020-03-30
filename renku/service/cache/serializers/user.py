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
"""Renku service cache user related models."""
from marshmallow import Schema, fields, post_load

from renku.service.cache.models.user import User


class UserSchema(Schema):
    """User identity schema."""

    user_id = fields.String(required=True)
    fullname = fields.String()
    email = fields.String()
    token = fields.String()

    @post_load
    def make_job(self, data, **options):
        """Construct job object."""
        return User(**data)
