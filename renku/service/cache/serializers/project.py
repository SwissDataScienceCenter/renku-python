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
"""Renku service cache project related serializers."""
import uuid
from datetime import datetime

from marshmallow import Schema, fields, post_load

from renku.service.cache.models.project import Project


class ProjectSchema(Schema):
    """Context schema for project clone."""

    created_at = fields.DateTime(missing=datetime.utcnow)

    project_id = fields.String(missing=lambda: uuid.uuid4().hex)
    user_id = fields.String(required=True)

    clone_depth = fields.Integer()
    git_url = fields.String()

    name = fields.String(required=True)
    fullname = fields.String(required=True)
    email = fields.String(required=True)
    owner = fields.String(required=True)
    token = fields.String(required=True)
    initialized = fields.Boolean(default=False)

    @post_load
    def make_project(self, data, **options):
        """Construct project object."""
        return Project(**data)
