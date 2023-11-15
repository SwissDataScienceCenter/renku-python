# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Renku service metadata versions list serializers."""
from marshmallow import Schema, fields


class VersionsListVersion(Schema):
    """A single instance of renku metadata version information."""

    version = fields.String()
    data = fields.Nested(Schema.from_dict({"metadata_version": fields.String()}))


class VersionsListResponse(Schema):
    """Lists all available renku metadata versions."""

    name = fields.String()
    versions = fields.List(fields.Nested(VersionsListVersion))


class VersionsListResponseRPC(Schema):
    """Versions list response RPC schema."""

    result = fields.Nested(VersionsListResponse)
