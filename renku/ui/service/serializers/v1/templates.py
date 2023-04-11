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
"""Renku service template serializers."""

from marshmallow import Schema, fields

from renku.ui.service.serializers.rpc import JsonRPCResponse


class ManifestTemplateSchema_1_5(Schema):
    """Manifest template schema."""

    description = fields.String(required=True, metadata={"description": "Description of the template"})
    folder = fields.String(required=True, metadata={"description": "Folder the template resides in"})
    name = fields.String(required=True, metadata={"description": "Name of the template"})
    variables = fields.Dict(
        load_default={}, metadata={"description": "Dictionary of values that can be set on this template"}
    )
    icon = fields.String(
        load_default=None, metadata={"description": "base64 encoded icon for the template in PNG format"}
    )


class ManifestTemplatesResponse_1_5(Schema):
    """Manifest templates response."""

    templates = fields.List(fields.Nested(ManifestTemplateSchema_1_5), required=True)


class ManifestTemplatesResponseRPC_1_5(JsonRPCResponse):
    """RPC schema for listing manifest templates."""

    result = fields.Nested(ManifestTemplatesResponse_1_5)
