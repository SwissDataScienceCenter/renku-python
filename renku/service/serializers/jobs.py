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
"""Renku service cache serializers for jobs."""
from marshmallow import Schema, fields

from renku.service.serializers.rpc import JsonRPCResponse


class JobDetails(Schema):
    """Response job details."""

    created_at = fields.DateTime()
    updated_at = fields.DateTime()

    job_id = fields.String(required=True)

    state = fields.String()
    extras = fields.Dict()


class JobListResponse(Schema):
    """Response schema for job listing."""

    jobs = fields.List(fields.Nested(JobDetails), required=True)


class JobListResponseRPC(JsonRPCResponse):
    """RPC response schema for jobs listing."""

    result = fields.Nested(JobListResponse)


class JobDetailsResponseRPC(Schema):
    """Response schema for job details."""

    result = fields.Nested(JobDetails)
