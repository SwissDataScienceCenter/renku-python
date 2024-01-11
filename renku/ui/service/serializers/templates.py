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
"""Renku service template serializers."""

from urllib.parse import urlparse

import yagup
from marshmallow import Schema, ValidationError, fields, post_load, pre_load, validates
from yagup.exceptions import InvalidURL

from renku.core.util.os import normalize_to_ascii
from renku.domain_model.dataset import ImageObjectRequestJson
from renku.ui.service.config import TEMPLATE_CLONE_DEPTH_DEFAULT
from renku.ui.service.errors import UserRepoUrlInvalidError
from renku.ui.service.serializers.common import GitUrlResponseMixin
from renku.ui.service.serializers.rpc import JsonRPCResponse
from renku.ui.service.utils import normalize_git_url


class ManifestTemplatesRequest(Schema):
    """Request schema for listing manifest templates."""

    template_git_url = fields.String(required=True, metadata={"description": "Template git repository url."})
    depth = fields.Integer(load_default=TEMPLATE_CLONE_DEPTH_DEFAULT)
    ref = fields.String(load_default=None, metadata={"description": "Remote git branch (or tag or commit SHA)."})

    @pre_load
    def set_fields(self, data, **_):
        """Set `ref` field from `branch` if present and set template url."""
        if "branch" in data and not data.get("ref"):
            # Backward compatibility: branch and ref were both used. Let's keep branch as the exposed field
            # even if internally it gets converted to "ref" later.
            data["ref"] = data["branch"]
            del data["branch"]
        if "url" in data and not data.get("template_git_url"):
            # needed for tests that share a fixture
            data["template_git_url"] = data["url"]

        return data

    @validates("template_git_url")
    def validate_template_git_url(self, value):
        """Validates git url."""
        if value:
            try:
                yagup.parse(value)
            except InvalidURL as e:
                raise UserRepoUrlInvalidError(e, "Invalid `template_git_url`")

        return value


class TemplateParameterSchema(Schema):
    """Manifest template schema."""

    key = fields.String(required=True)
    value = fields.String(load_default="")


class ProjectTemplateRequest(ManifestTemplatesRequest):
    """Request schema for listing manifest templates."""

    token = fields.String()
    identifier = fields.String(required=True, metadata={"description": "Indentifier of the template"})
    initial_branch = fields.String(
        load_default=None, metadata={"description": "Name for the initial branch in the new project."}
    )
    parameters = fields.List(
        fields.Nested(TemplateParameterSchema), load_default=[], metadata={"description": "Template parameters"}
    )
    project_name = fields.String(required=True, metadata={"description": "Project name"})
    project_namespace = fields.String(required=True, metadata={"description": "Project namespace"})
    project_repository = fields.String(required=True, metadata={"description": "Project remote repository"})
    project_description = fields.String(load_default=None, metadata={"description": "Project description"})
    project_keywords = fields.List(fields.String(), load_default=None, metadata={"description": "Project keywords"})
    project_custom_metadata = fields.Dict(
        load_default=None, metadata={"description": "Project custom JSON-LD metadata"}
    )
    data_directory = fields.String(
        load_default=None, metadata={"description": "Base dataset data directory in project. Defaults to 'data/'"}
    )
    image = fields.Nested(ImageObjectRequestJson, load_default=None)
    slug = fields.String()
    fullname = fields.String()
    email = fields.String()

    @post_load()
    def add_required_fields(self, data, **kwargs):
        """Add necessary fields."""
        if "template_git_url" in data and data["template_git_url"]:
            data["template_git_url"] = normalize_git_url(data["template_git_url"])
        project_name_stripped = normalize_to_ascii(data["project_name"])
        project_name_stripped = normalize_git_url(project_name_stripped)
        if len(project_name_stripped) == 0:
            raise ValidationError("Project name contains only unsupported characters")
        new_project_url = f"{data['project_repository']}/{data['project_namespace']}/{project_name_stripped}"
        try:
            _ = yagup.GitURL.parse(new_project_url)
        except InvalidURL as e:
            raise ValidationError(
                "`project_repository`, `project_namespace` and `project_name` do not form a valid git url"
            ) from e

        project_slug = f"{data['project_namespace']}/{project_name_stripped}"
        data["new_project_url"] = new_project_url
        data["project_name_stripped"] = project_name_stripped
        data["project_slug"] = project_slug

        new_project_url_parsed = urlparse(new_project_url)
        url = "oauth2:{}@{}".format(data["token"], new_project_url_parsed.netloc)
        data["new_project_url_with_auth"] = new_project_url_parsed._replace(netloc=url).geturl()

        return data


class ManifestTemplateSchema(Schema):
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
    ssh_supported = fields.Boolean(
        load_default=False, metadata={"description": "Whether the project supports SSH connections to sessions or not."}
    )


class ManifestTemplatesResponse(Schema):
    """Manifest templates response."""

    templates = fields.List(fields.Nested(ManifestTemplateSchema), required=True)


class ManifestTemplatesResponseRPC(JsonRPCResponse):
    """RPC schema for listing manifest templates."""

    result = fields.Nested(ManifestTemplatesResponse)


class ProjectTemplateResponse(GitUrlResponseMixin):
    """Response schema for dataset list view."""

    url = fields.String(required=True)
    namespace = fields.String(required=True)
    name = fields.String(required=True)
    slug = fields.String(required=True)
    project_id = fields.String(required=False, dump_default=None)


class ProjectTemplateResponseRPC(JsonRPCResponse):
    """RPC schema for project creation."""

    result = fields.Nested(ProjectTemplateResponse)
