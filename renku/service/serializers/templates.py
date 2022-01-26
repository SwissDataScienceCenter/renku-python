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
"""Renku service template serializers."""

from urllib.parse import urlparse

from marshmallow import Schema, ValidationError, fields, post_load, pre_load, validates
from yagup import GitURL
from yagup.exceptions import InvalidURL

from renku.core.utils.os import normalize_to_ascii
from renku.service.config import TEMPLATE_CLONE_DEPTH_DEFAULT
from renku.service.serializers.cache import ProjectCloneContext, RepositoryCloneRequest
from renku.service.serializers.rpc import JsonRPCResponse


class ManifestTemplatesRequest(RepositoryCloneRequest):
    """Request schema for listing manifest templates."""

    url = fields.String(required=True)
    ref = fields.String(missing=None)
    depth = fields.Integer(missing=TEMPLATE_CLONE_DEPTH_DEFAULT)

    @pre_load()
    def set_git_url(self, data, **kwargs):
        """Set git_url field."""
        data["git_url"] = data["url"]
        return data


class TemplateParameterSchema(Schema):
    """Manifest template schema."""

    key = fields.String(required=True)
    value = fields.String(missing="")


class ProjectTemplateRequest(ProjectCloneContext, ManifestTemplatesRequest):
    """Request schema for listing manifest templates."""

    identifier = fields.String(required=True)
    parameters = fields.List(fields.Nested(TemplateParameterSchema), missing=[])

    project_name = fields.String(required=True)
    project_namespace = fields.String(required=True)
    project_repository = fields.String(required=True)
    project_slug = fields.String(required=True)
    project_description = fields.String(missing=None)
    project_keywords = fields.List(fields.String(), missing=None)
    project_custom_metadata = fields.Dict(missing=None)

    new_project_url = fields.String(required=True)
    project_name_stripped = fields.String(required=True)

    initial_branch = fields.String(missing=None)

    @pre_load()
    def create_new_project_url(self, data, **kwargs):
        """Set owner and name fields."""
        try:
            project_name_stripped = normalize_to_ascii(data["project_name"])
            if len(project_name_stripped) == 0:
                raise ValidationError("Project name contains only unsupported characters")
            new_project_url = f"{data['project_repository']}/{data['project_namespace']}/{project_name_stripped}"
            _ = GitURL.parse(new_project_url)
        except InvalidURL as e:
            raise ValidationError("`git_url` contains unsupported characters") from e

        project_slug = f"{data['project_namespace']}/{project_name_stripped}"
        data["new_project_url"] = new_project_url
        data["project_name_stripped"] = project_name_stripped
        data["project_slug"] = project_slug

        return data

    @validates("new_project_url")
    def validate_new_project_url(self, value):
        """Validates git url."""
        try:
            GitURL.parse(value)
        except InvalidURL as e:
            raise ValidationError(str(e))

        return value

    @post_load()
    def format_new_project_url(self, data, **kwargs):
        """Format URL with an access token."""
        new_project_url = urlparse(data["new_project_url"])

        url = "oauth2:{0}@{1}".format(data["token"], new_project_url.netloc)
        data["new_project_url_with_auth"] = new_project_url._replace(netloc=url).geturl()

        return data


class ManifestTemplateSchema(Schema):
    """Manifest template schema."""

    description = fields.String(required=True, description="Description of the template")
    folder = fields.String(required=True, description="Folder the template resides in")
    name = fields.String(required=True, description="Name of the template")
    variables = fields.Dict(missing={}, description="Dictionary of values that can be set on this template")
    icon = fields.String(missing=None, description="base64 encoded icon for the template in PNG format")


class ManifestTemplatesResponse(Schema):
    """Manifest templates response."""

    templates = fields.List(fields.Nested(ManifestTemplateSchema), required=True)


class ManifestTemplatesResponseRPC(JsonRPCResponse):
    """RPC schema for listing manifest templates."""

    result = fields.Nested(ManifestTemplatesResponse)


class ProjectTemplateResponse(Schema):
    """Response schema for dataset list view."""

    url = fields.String(required=True)
    namespace = fields.String(required=True)
    name = fields.String(required=True)
    slug = fields.String(required=True)
    project_id = fields.String(required=False, default=None)


class ProjectTemplateResponseRPC(JsonRPCResponse):
    """RPC schema for project creation."""

    result = fields.Nested(ProjectTemplateResponse)
