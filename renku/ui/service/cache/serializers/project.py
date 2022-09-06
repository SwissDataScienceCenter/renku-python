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

from marshmallow import fields, post_load

from renku.ui.service.cache.models.project import Project
from renku.ui.service.serializers.common import CreationSchema, MandatoryUserSchema


class ProjectSchema(CreationSchema, MandatoryUserSchema):
    """Context schema for project clone."""

    last_fetched_at = fields.DateTime(load_default=datetime.utcnow)

    project_id = fields.String(load_default=lambda: uuid.uuid4().hex)

    clone_depth = fields.Integer()
    git_url = fields.String()

    name = fields.String(required=True)
    slug = fields.String(required=True)
    description = fields.String(load_default=None)
    fullname = fields.String(required=True)
    email = fields.String(required=True)
    owner = fields.String(required=True)
    token = fields.String(required=True)
    initialized = fields.Boolean(dump_default=False)

    @post_load
    def make_project(self, data, **options):
        """Construct project object."""
        return Project(**data)
