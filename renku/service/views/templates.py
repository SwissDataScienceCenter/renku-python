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
"""Renku service templates view."""
from flask import Blueprint, request
from flask_apispec import marshal_with, use_kwargs

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.templates_create_project import TemplatesCreateProjectCtrl
from renku.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl
from renku.service.serializers.templates import (
    ManifestTemplatesRequest,
    ManifestTemplatesResponseRPC,
    ProjectTemplateRequest,
    ProjectTemplateResponseRPC,
)
from renku.service.views.decorators import (
    accepts_json,
    handle_common_except,
    header_doc,
    requires_cache,
    requires_identity,
)

TEMPLATES_BLUEPRINT_TAG = "templates"
templates_blueprint = Blueprint(TEMPLATES_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@use_kwargs(ManifestTemplatesRequest, location="query")
@marshal_with(ManifestTemplatesResponseRPC)
@header_doc("Clone a remote template repository and read the templates.", tags=(TEMPLATES_BLUEPRINT_TAG,))
@templates_blueprint.route(
    "/templates.read_manifest", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def read_manifest_from_template(user_data, cache):
    """Read templates from the manifest file of a template repository."""
    return TemplatesReadManifestCtrl(cache, user_data, dict(request.args)).to_response()


@use_kwargs(ProjectTemplateRequest)
@marshal_with(ProjectTemplateResponseRPC)
@header_doc(
    "Create a new project starting from a target template available in a " "remote repositpry.",
    tags=(TEMPLATES_BLUEPRINT_TAG,),
)
@templates_blueprint.route(
    "/templates.create_project", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def create_project_from_template(user_data, cache):
    """Create a new project starting form target template."""
    return TemplatesCreateProjectCtrl(cache, user_data, dict(request.json)).to_response()
