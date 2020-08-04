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

import shutil

from flask import Blueprint, request
from flask_apispec import marshal_with, use_kwargs
from marshmallow import EXCLUDE

from renku.core.commands.init import create_from_template_local, read_template_manifest
from renku.core.utils.contexts import chdir
from renku.service.config import INVALID_PARAMS_ERROR_CODE, SERVICE_PREFIX
from renku.service.serializers.templates import (
    ManifestTemplatesRequest,
    ManifestTemplatesResponseRPC,
    ProjectTemplateRequest,
    ProjectTemplateResponseRPC,
)
from renku.service.utils import make_new_project_path, new_repo_push
from renku.service.views import error_response, result_response
from renku.service.views.cache import _project_clone
from renku.service.views.decorators import (
    accepts_json,
    handle_base_except,
    handle_git_except,
    handle_renku_except,
    handle_schema_except,
    handle_validation_except,
    header_doc,
    requires_cache,
    requires_identity,
)

TEMPLATES_BLUEPRINT_TAG = "templates"
templates_blueprint = Blueprint(TEMPLATES_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@use_kwargs(ManifestTemplatesRequest, locations=["query"])
@marshal_with(ManifestTemplatesResponseRPC)
@header_doc("Clone a remote template repository and read the templates.", tags=(TEMPLATES_BLUEPRINT_TAG,))
@templates_blueprint.route(
    "/templates.read_manifest", methods=["GET"], provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@handle_schema_except
@accepts_json
@requires_cache
@requires_identity
def read_manifest_from_template(user, cache):
    """Read templates from the manifest file of a template repository."""
    project_data = ManifestTemplatesRequest().load({**user, **request.args,}, unknown=EXCLUDE)
    project = _project_clone(user, project_data)
    manifest = read_template_manifest(project.abs_path)

    return result_response(ManifestTemplatesResponseRPC(), {"templates": manifest})


@use_kwargs(ProjectTemplateRequest)
@marshal_with(ProjectTemplateRequest)
@header_doc(
    "Create a new project starting from a target template available in a " "remote repositpry.",
    tags=(TEMPLATES_BLUEPRINT_TAG,),
)
@templates_blueprint.route(
    "/templates.create_project", methods=["POST"], provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@handle_schema_except
@accepts_json
@requires_cache
@requires_identity
def create_project_from_template(user, cache):
    """Create a new project starting form target template."""
    ctx = ProjectTemplateRequest().load({**user, **request.json,}, unknown=EXCLUDE)

    # Clone project and find target template
    template_project = _project_clone(user, ctx)
    templates = read_template_manifest(template_project.abs_path)
    template = next((template for template in templates if template["folder"] == ctx["identifier"]), None)
    if template is None:
        return error_response(INVALID_PARAMS_ERROR_CODE, "invalid identifier for target repository")

    # Verify missing parameters
    template_parameters = template.get("variables", {})
    provided_parameters = {p["key"]: p["value"] for p in ctx["parameters"]}
    missing_keys = list(template_parameters.keys() - provided_parameters.keys())
    if len(missing_keys) > 0:
        return error_response(INVALID_PARAMS_ERROR_CODE, f"missing parameter: {missing_keys[0]}")

    # Create new path
    new_project_path = make_new_project_path(user, ctx)
    if new_project_path.exists():
        shutil.rmtree(str(new_project_path))
    new_project_path.mkdir(parents=True, exist_ok=True)

    # prepare data and init new project
    source_path = template_project.abs_path / ctx["identifier"]
    git_user = {"email": user["email"], "name": user["fullname"]}
    with chdir(new_project_path):
        create_from_template_local(
            source_path, ctx["project_name"], provided_parameters, git_user, ctx["url"], ctx["ref"], "service"
        )
    new_repo_push(new_project_path, ctx["new_project_url_with_auth"])

    resp = {
        "url": ctx["new_project_url"],
        "namespace": ctx["project_namespace"],
        "name": ctx["project_name_stripped"],
    }
    return result_response(ProjectTemplateResponseRPC(), resp)
