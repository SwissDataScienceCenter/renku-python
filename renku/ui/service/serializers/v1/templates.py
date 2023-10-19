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

from marshmallow import Schema, ValidationError, fields, post_load, pre_load
from yagup import GitURL
from yagup.exceptions import InvalidURL

from renku.core.util.os import normalize_to_ascii
from renku.domain_model.dataset import ImageObjectRequestJson
from renku.ui.service.config import TEMPLATE_CLONE_DEPTH_DEFAULT
from renku.ui.service.serializers.cache import ProjectCloneContext, RepositoryCloneRequest
from renku.ui.service.serializers.rpc import JsonRPCResponse
from renku.ui.service.serializers.templates import TemplateParameterSchema
from renku.ui.service.utils import normalize_git_url


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


class ManifestTemplatesRequest_v2_2(RepositoryCloneRequest):
    """Request schema for listing manifest templates."""

    url = fields.String(required=True)
    depth = fields.Integer(load_default=TEMPLATE_CLONE_DEPTH_DEFAULT)

    @pre_load()
    def set_git_url(self, data, **kwargs):
        """Set git_url field."""
        if data.get("url"):
            data["git_url"] = data.get("url")
        data["template_git_url"] = data["git_url"]
        return data


class ProjectTemplateRequest_v2_2(ProjectCloneContext, ManifestTemplatesRequest_v2_2):
    """Request schema for listing manifest templates."""

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

    @post_load()
    def add_required_fields(self, data, **kwargs):
        """Add necessary fields."""
        data["template_git_url"] = data.get("git_url")
        project_name_stripped = normalize_to_ascii(data["project_name"])
        project_name_stripped = normalize_git_url(project_name_stripped)
        if len(project_name_stripped) == 0:
            raise ValidationError("Project name contains only unsupported characters")
        new_project_url = f"{data['project_repository']}/{data['project_namespace']}/{project_name_stripped}"
        try:
            _ = GitURL.parse(new_project_url)
        except InvalidURL as e:
            raise ValidationError("`git_url` contains unsupported characters") from e

        project_slug = f"{data['project_namespace']}/{project_name_stripped}"
        data["new_project_url"] = new_project_url
        data["project_name_stripped"] = project_name_stripped
        data["project_slug"] = project_slug

        new_project_url_parsed = urlparse(new_project_url)
        url = "oauth2:{}@{}".format(data["token"], new_project_url_parsed.netloc)
        data["new_project_url_with_auth"] = new_project_url_parsed._replace(netloc=url).geturl()

        return data
