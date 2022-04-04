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
"""Renku service cache serializers."""

from marshmallow import Schema, fields

from renku.ui.service.serializers.rpc import JsonRPCResponse


class ProjectMigrationCheckResponse_0_9(Schema):
    """Response schema for project migration check."""

    migration_required = fields.Boolean(attribute="core_compatibility_status.migration_required")
    template_update_possible = fields.Boolean(attribute="template_status.newer_template_available")
    automated_template_update = fields.Boolean(attribute="template_status.automated_template_update")
    current_template_version = fields.String(attribute="template_status.project_template_version")
    latest_template_version = fields.String(attribute="template_status.latest_template_version")
    template_source = fields.String(attribute="template_status.template_source")
    template_ref = fields.String(attribute="template_status.template_ref")
    template_id = fields.String(attribute="template_status.template_id")
    docker_update_possible = fields.Boolean(attribute="dockerfile_renku_status.newer_renku_available")

    project_supported = fields.Boolean()
    project_renku_version = fields.String(data_key="project_version")
    core_renku_version = fields.String(data_key="latest_version")


class ProjectMigrationCheckResponseRPC_0_9(JsonRPCResponse):
    """RPC response schema for project migration check."""

    result = fields.Nested(ProjectMigrationCheckResponse_0_9)
