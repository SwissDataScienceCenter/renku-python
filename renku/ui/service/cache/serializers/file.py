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

from marshmallow import fields, post_load

from renku.ui.service.cache.models.file import File, FileChunk
from renku.ui.service.serializers.common import CreationSchema, FileDetailsSchema, MandatoryUserSchema


class FileSchema(FileDetailsSchema, MandatoryUserSchema):
    """Schema for file model."""

    override_existing = fields.Boolean(
        load_default=False,
        metadata={"description": "Overried files. Useful when extracting from archives."},
    )

    @post_load
    def make_file(self, data, **options):
        """Construct file object."""
        return File(**data)


class FileChunkSchema(CreationSchema, MandatoryUserSchema):
    """Schema for file model."""

    chunk_file_id = fields.String(load_default=lambda: uuid.uuid4().hex)
    file_name = fields.String(required=True)

    chunked_id = fields.String(required=True)
    relative_path = fields.String(required=True)

    @post_load
    def make_file(self, data, **options):
        """Construct file object."""
        return FileChunk(**data)
