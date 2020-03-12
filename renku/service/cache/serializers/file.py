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
"""Renku service file cache serializers."""
import uuid
from datetime import datetime

from marshmallow import Schema, fields, post_load

from renku.service.cache.models.file import File


class FileSchema(Schema):
    """Schema for file model."""

    created_at = fields.DateTime(missing=datetime.utcnow)

    file_id = fields.String(missing=lambda: uuid.uuid4().hex)
    user_id = fields.String(required=True)

    content_type = fields.String(missing='unknown')
    file_name = fields.String(required=True)

    # measured in bytes (comes from stat() - st_size)
    file_size = fields.Integer(required=True)

    relative_path = fields.String(required=True)
    is_archive = fields.Boolean(missing=False)
    is_dir = fields.Boolean(required=True)
    unpack_archive = fields.Boolean(missing=False)
    override_existing = fields.Boolean(missing=False)

    @post_load
    def make_file(self, data, **options):
        """Construct file object."""
        return File(**data)
