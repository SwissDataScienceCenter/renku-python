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
"""Renku service cache serializers."""

from marshmallow import Schema, fields

from renku.ui.service.serializers.cache import DockerfileStatusResponse, ProjectCompatibilityResponse
from renku.ui.service.serializers.common import RenkuSyncSchema
from renku.ui.service.serializers.rpc import JsonRPCResponse


class ProjectMigrateResponse_1_0(RenkuSyncSchema):
    """Response schema for project migrate."""

    was_migrated = fields.Boolean()
    template_migrated = fields.Boolean()
    docker_migrated = fields.Boolean()
    messages = fields.List(fields.String)


class ProjectMigrateResponseRPC_1_0(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateResponse_1_0)


class TemplateStatusResponse_1_5(Schema):
    """Response schema outlining template status for migrations check."""

    automated_template_update = fields.Boolean(
        metadata={"description": "Whether or not the project template explicitly supports automated updates."}
    )
    newer_template_available = fields.Boolean(
        metadata={
            "description": "Whether or not the current version of the project template differs from the "
            "one used in the project."
        }
    )
    template_source = fields.String(
        metadata={
            "description": "Source of the template repository, "
            "either a Git URL or 'renku' if an embedded template was used."
        }
    )
    template_ref = fields.String(
        metadata={
            "description": "The branch/tag/commit from the template_source repository "
            "that was used to create this project."
        }
    )
    template_id = fields.String(metadata={"description": "The id of the template in the template repository."})

    project_template_version = fields.String(
        allow_none=True, metadata={"description": "The version of the template last used in the user's project."}
    )
    latest_template_version = fields.String(
        allow_none=True, metadata={"description": "The current version of the template in the template repository."}
    )


class ProjectMigrationCheckResponse_1_5(Schema):
    """Response schema for project migration check."""

    project_supported = fields.Boolean(
        metadata={
            "description": "Determines whether this project is a Renku project that is supported by the version "
            "running on this service (not made with a newer version)."
        }
    )
    core_renku_version = fields.String(metadata={"description": "Version of Renku running in this service."})
    project_renku_version = fields.String(metadata={"description": "Version of Renku last used to change the project."})

    core_compatibility_status = fields.Nested(
        ProjectCompatibilityResponse,
        metadata={"description": "Fields detailing the compatibility of the project with this core service version."},
    )
    dockerfile_renku_status = fields.Nested(
        DockerfileStatusResponse,
        metadata={"description": "Fields detailing the status of the Dockerfile in the project."},
    )
    template_status = fields.Nested(
        TemplateStatusResponse_1_5,
        metadata={"description": "Fields detailing the status of the project template used by this project."},
    )


class ProjectMigrationCheckResponseRPC_1_5(JsonRPCResponse):
    """RPC response schema for project migration check."""

    result = fields.Nested(ProjectMigrationCheckResponse_1_5)
