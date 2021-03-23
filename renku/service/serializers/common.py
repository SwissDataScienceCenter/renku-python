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
"""Renku service parent serializers."""
from marshmallow import Schema, fields

from renku.service.serializers.rpc import JsonRPCResponse


class RenkuSyncSchema(Schema):
    """Parent schema for all Renku write operations."""

    remote_branch = fields.String()


class RepositoryContext(Schema):
    """Parent schema for Renku repository support."""

    git_url = fields.String()
    project_id = fields.String()

    ref = fields.String()
    commit_message = fields.String()
    client_extras = fields.String()

    is_delayed = fields.Boolean()

    # NOTE: Migration execution is always false by default, unless explicitly asked by a client.
    migrate_project = fields.Boolean(default=False, missing=False)


class JobDetailsResponse(Schema):
    """Response schema for enqueued job."""

    job_id = fields.String()
    created_at = fields.DateTime()


class DelayedResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(JobDetailsResponse)
