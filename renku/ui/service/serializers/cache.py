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
import time
import uuid
from urllib.parse import urlparse

from marshmallow import Schema, ValidationError, fields, post_load, pre_load, validates_schema
from werkzeug.utils import secure_filename

from renku.core import errors
from renku.core.util.os import normalize_to_ascii
from renku.domain_model.git import GitURL
from renku.ui.service.config import PROJECT_CLONE_DEPTH_DEFAULT
from renku.ui.service.serializers.common import (
    ArchiveSchema,
    AsyncSchema,
    FileDetailsSchema,
    LocalRepositorySchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.ui.service.serializers.rpc import JsonRPCResponse


def extract_file(request):
    """Extract file from Flask request.

    Raises:
        `ValidationError`: If file data or filename is missing in request.
    """
    files = request.files
    if "file" not in files:
        raise ValidationError("missing key: file")

    file = files["file"]
    if file and not file.filename:
        raise ValidationError("wrong filename: {0}".format(file.filename))

    if file:
        file.filename = secure_filename(file.filename)
        return file


class FileUploadRequest(ArchiveSchema):
    """Request schema for file upload."""

    override_existing = fields.Boolean(
        load_default=False,
        metadata={"description": "Overried files. Useful when extracting from archives."},
    )

    chunked_id = fields.String(data_key="dzuuid", load_default=None, metadata={"description": "Dropzone upload id."})
    chunk_index = fields.Integer(
        data_key="dzchunkindex", load_default=None, metadata={"description": "Dropzone chunk index."}
    )
    chunk_count = fields.Integer(
        data_key="dztotalchunkcount", load_default=None, metadata={"description": "Dropzone total chunk count."}
    )
    chunk_size = fields.Integer(
        data_key="dzchunksize", load_default=None, metadata={"description": "Dropzone chunk size."}
    )
    chunk_byte_offset = fields.Integer(
        data_key="dzchunkbyteoffset", load_default=None, metadata={"description": "Dropzone chunk offset."}
    )
    chunked_content_type = fields.String(
        load_default=None, metadata={"description": "Content type of file file for chunked uploads."}
    )
    total_size = fields.Integer(
        data_key="dztotalfilesize", load_default=None, metadata={"description": "Dropzone total file size."}
    )

    @validates_schema
    def validate_requires(self, data, **kwargs):
        """Validate chunk upload data."""
        if data.get("dzuuid") is not None and ("dzchunkindex" not in data or "dztotalchunkcount" not in data):
            raise ValidationError("'dzchunkindex' and 'dztotalchunkcount' are required when 'dzuuid' is set.")


class FileUploadResponse(Schema):
    """Response schema for file upload."""

    files = fields.List(fields.Nested(FileDetailsSchema))


class FileUploadResponseRPC(JsonRPCResponse):
    """RPC response schema for file upload response."""

    result = fields.Nested(FileUploadResponse)


class FileChunksDeleteRequest(Schema):
    """Request schema for deleting uploaded chunks."""

    chunked_id = fields.String(data_key="dzuuid", load_default=None, metadata={"description": "Dropzone upload id."})


class FileChunksDeleteResponseRPC(JsonRPCResponse):
    """RPC response schema for file upload response."""

    result = fields.String()


class FileListResponse(Schema):
    """Response schema for files listing."""

    files = fields.List(fields.Nested(FileDetailsSchema), required=True)


class FileListResponseRPC(JsonRPCResponse):
    """RPC response schema for files listing."""

    result = fields.Nested(FileListResponse)


class RepositoryCloneRequest(RemoteRepositorySchema):
    """Request schema for repository clone."""

    depth = fields.Integer(metadata={"description": "Git fetch depth"}, load_default=PROJECT_CLONE_DEPTH_DEFAULT)
    ref = fields.String(metadata={"description": "Repository reference (branch, commit or tag)"}, load_default=None)


class ProjectCloneContext(RepositoryCloneRequest):
    """Context schema for project clone."""

    # measured in ms
    timestamp = fields.Integer(load_default=time.time() * 1e3)

    # user data
    name = fields.String()
    slug = fields.String()
    fullname = fields.String()
    email = fields.String()
    owner = fields.String()
    token = fields.String()

    @pre_load()
    def set_missing_id(self, data, **kwargs):
        """Set project_id when missing."""
        if not data.get("project_id"):
            data["project_id"] = uuid.uuid4().hex

        return data

    @pre_load()
    def set_owner_name(self, data, **kwargs):
        """Set owner and name fields."""
        try:
            git_url = GitURL.parse(data["git_url"])
        except UnicodeError as e:
            raise ValidationError("`git_url` contains unsupported characters") from e
        except errors.InvalidGitURL as e:
            raise ValidationError("Invalid `git_url`") from e

        if git_url.owner is None:
            raise ValidationError("Invalid `git_url`")
        data["owner"] = git_url.owner

        if git_url.name is None:
            raise ValidationError("Invalid `git_url`")
        data["name"] = git_url.name
        data["slug"] = normalize_to_ascii(data["name"])

        return data

    def format_url(self, data):
        """Format url with auth."""
        git_url = urlparse(data["git_url"])

        url = "oauth2:{0}@{1}".format(data["token"], git_url.netloc)
        return git_url._replace(netloc=url).geturl()

    @post_load
    def finalize_data(self, data, **kwargs):
        """Finalize data."""
        data["url_with_auth"] = self.format_url(data)

        if not data["depth"]:
            # NOTE: In case of `depth=None` or `depth=0` we set to default depth.
            data["depth"] = PROJECT_CLONE_DEPTH_DEFAULT

        try:
            depth = int(data["depth"])

            if depth < 0:
                # NOTE: In case of `depth<0` we remove the depth limit.
                data["depth"] = None

        except ValueError:
            data["depth"] = PROJECT_CLONE_DEPTH_DEFAULT

        return data


class ProjectCloneResponse(Schema):
    """Response schema for project clone."""

    project_id = fields.String(required=True)
    git_url = fields.String(required=True)
    initialized = fields.Boolean(dump_default=False)


class ProjectCloneResponseRPC(JsonRPCResponse):
    """RPC response schema for project clone response."""

    result = fields.Nested(ProjectCloneResponse)


class ProjectListResponse(Schema):
    """Response schema for project listing."""

    projects = fields.List(fields.Nested(ProjectCloneResponse), required=True)


class ProjectListResponseRPC(JsonRPCResponse):
    """RPC response schema for project listing."""

    result = fields.Nested(ProjectListResponse)


class ProjectMigrateRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for project migrate."""

    force_template_update = fields.Boolean(dump_default=False)
    skip_template_update = fields.Boolean(dump_default=False)
    skip_docker_update = fields.Boolean(dump_default=False)
    skip_migrations = fields.Boolean(dump_default=False)

    @pre_load()
    def handle_ref(self, data, **kwargs):
        """Handle ref and branch."""

        # Backward compatibility: branch and ref were both used. Let's keep branch as the exposed field
        # even if interally it gets converted to "ref" later.
        if data.get("ref"):
            data["branch"] = data["ref"]
            del data["ref"]

        return data


class ProjectMigrateResponse(RenkuSyncSchema):
    """Response schema for project migrate."""

    was_migrated = fields.Boolean()
    template_migrated = fields.Boolean()
    docker_migrated = fields.Boolean()
    messages = fields.List(fields.String)
    warnings = fields.List(fields.String)
    errors = fields.List(fields.String)


class ProjectMigrateResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateResponse)


class ProjectMigrationCheckRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for project migration check."""


class ProjectCompatibilityResponse(Schema):
    """Response schema outlining service compatibility for migrations check."""

    project_metadata_version = fields.String(
        metadata={"description": "Current version of the Renku metadata in the project."}
    )
    current_metadata_version = fields.String(
        metadata={"description": "Highest metadata version supported by this service."}
    )
    migration_required = fields.Boolean(
        metadata={"description": "Whether or not a metadata migration is required to be compatible with this service."}
    )


class DockerfileStatusResponse(Schema):
    """Response schema outlining dockerfile status for migrations check."""

    newer_renku_available = fields.Boolean(
        metadata={
            "description": "Whether the version of Renku in this service is newer than the one in the Dockerfile."
        }
    )
    automated_dockerfile_update = fields.Boolean(
        metadata={"description": "Whether or not the Dockerfile supports automated Renku version updates."}
    )
    latest_renku_version = fields.String(
        metadata={"description": "The current version of Renku available in this service."}
    )
    dockerfile_renku_version = fields.String(metadata={"description": "Version of Renku specified in the Dockerfile."})


class TemplateStatusResponse(Schema):
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


class ProjectMigrationCheckResponse(Schema):
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
        TemplateStatusResponse,
        metadata={"description": "Fields detailing the status of the project template used by this project."},
    )


class ProjectMigrationCheckResponseRPC(JsonRPCResponse):
    """RPC response schema for project migration check."""

    result = fields.Nested(ProjectMigrationCheckResponse)
