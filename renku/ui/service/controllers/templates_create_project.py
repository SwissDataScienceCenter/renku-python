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
"""Renku service template create project controller."""
import shutil
from typing import Any, Dict, Optional, cast

from marshmallow import EXCLUDE

from renku.command.init import create_from_template_local_command
from renku.core import errors
from renku.core.template.template import fetch_templates_source
from renku.core.util.contexts import renku_project_context
from renku.domain_model.template import Template
from renku.infrastructure.repository import Repository
from renku.ui.service.config import MESSAGE_PREFIX
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.errors import UserProjectCreationError
from renku.ui.service.serializers.templates import ProjectTemplateRequest, ProjectTemplateResponseRPC
from renku.ui.service.utils import new_repo_push
from renku.ui.service.views import result_response
from renku.version import __version__, is_release


class TemplatesCreateProjectCtrl(ServiceCtrl, RenkuOperationMixin):
    """Template create project controller."""

    REQUEST_SERIALIZER = ProjectTemplateRequest()
    RESPONSE_SERIALIZER = ProjectTemplateResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a templates read manifest controller."""
        self.ctx = cast(
            Dict[str, Any],
            TemplatesCreateProjectCtrl.REQUEST_SERIALIZER.load({**user_data, **request_data}, unknown=EXCLUDE),
        )
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} init {self.ctx['project_name']}"
        super(TemplatesCreateProjectCtrl, self).__init__(cache, user_data, request_data)

        self.template: Optional[Template] = None

    @property
    def context(self):
        """Controller context."""
        return self.ctx

    @property
    def default_metadata(self):
        """Default metadata for project creation."""

        metadata = {
            "__template_source__": self.ctx["git_url"],
            "__template_ref__": self.ctx["ref"],
            "__template_id__": self.ctx["identifier"],
            "__namespace__": self.ctx["project_namespace"],
            "__repository__": self.ctx["project_repository"],
            "__sanitized_project_name__": self.ctx["project_name_stripped"],
            "__project_slug__": self.ctx["project_slug"],
            "__project_description__": self.ctx["project_description"],
        }
        if is_release():
            metadata["__renku_version__"] = __version__

        return metadata

    @property
    def git_user(self) -> Dict[str, str]:
        """Extract git user from the user data."""
        return {
            "email": self.user_data["email"],
            "name": self.user_data["fullname"],
        }

    def setup_new_project(self):
        """Setup new project for initialization."""
        # TODO: Request attribute naming on create project and read manifest is not consistent.
        new_project_data = {
            "clone_depth": self.ctx["depth"],
            "git_url": self.ctx["new_project_url"],
            "name": self.ctx["project_name"],
            "slug": self.ctx["project_name_stripped"],
            "description": self.ctx["project_description"],
            "fullname": self.ctx["fullname"],
            "email": self.ctx["email"],
            "owner": self.ctx["project_namespace"],
            "token": self.ctx["token"],
            "initialized": True,
        }
        project = self.cache.make_project(self.user, new_project_data)

        new_project_path = project.abs_path
        if new_project_path.exists():
            shutil.rmtree(new_project_path)

        new_project_path.mkdir(parents=True, exist_ok=True)

        return project

    def setup_template(self):
        """Reads template manifest."""
        templates_source = fetch_templates_source(source=self.ctx["git_url"], reference=self.ctx["ref"])
        identifier = self.ctx["identifier"]
        try:
            self.template = templates_source.get_template(id=identifier, reference=None)
        except (errors.InvalidTemplateError, errors.TemplateNotFoundError) as e:
            raise UserProjectCreationError(
                error_message=f"the template '{identifier}' does not exist in the target template's repository"
            ) from e

        repository = Repository(templates_source.path)
        self.template_version = repository.head.commit.hexsha

        # Verify missing parameters
        template_parameters = set(p.name for p in self.template.parameters)
        provided_parameters = {p["key"]: p["value"] for p in self.ctx["parameters"]}
        missing_keys = list(template_parameters - provided_parameters.keys())
        if len(missing_keys) > 0:
            raise UserProjectCreationError(error_message=f"the template requires a value for '${missing_keys[0]}'")

        return provided_parameters

    def new_project_push(self, project_path):
        """Push new project to the remote."""
        return new_repo_push(project_path, self.ctx["new_project_url_with_auth"])

    def new_project(self):
        """Create new project from template."""
        provided_parameters = self.setup_template()
        assert self.template is not None
        new_project = self.setup_new_project()
        new_project_path = new_project.abs_path

        with renku_project_context(new_project_path):
            create_from_template_local_command().build().execute(
                self.template.path,
                name=self.ctx["project_name"],
                namespace=self.ctx["project_namespace"],
                metadata=provided_parameters,
                default_metadata=self.default_metadata,
                custom_metadata=self.ctx["project_custom_metadata"],
                template_version=self.template_version,
                immutable_template_files=self.template.immutable_files,
                automated_template_update=self.template.allow_update,
                user=self.git_user,
                initial_branch=self.ctx["initial_branch"],
                commit_message=self.ctx["commit_message"],
                description=self.ctx["project_description"],
                data_dir=self.ctx.get("data_directory"),
            )

        self.new_project_push(new_project_path)

        new_project.initialized = True
        new_project.save()

        return {
            "url": self.ctx["new_project_url"],
            "namespace": self.ctx["project_namespace"],
            "name": self.ctx["project_name"],
            "slug": self.ctx["project_name_stripped"],
            "project_id": new_project.project_id,
        }

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(TemplatesCreateProjectCtrl.RESPONSE_SERIALIZER, self.new_project())
