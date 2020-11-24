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

import git
from marshmallow import EXCLUDE

from renku.core.commands.init import create_from_template_local, read_template_manifest
from renku.core.errors import RenkuException
from renku.core.utils.contexts import click_context
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.controllers.utils.project_clone import user_project_clone
from renku.service.serializers.templates import ProjectTemplateRequest, ProjectTemplateResponseRPC
from renku.service.utils import new_repo_push
from renku.service.views import result_response


class TemplatesCreateProjectCtrl(ServiceCtrl, ReadOperationMixin):
    """Template create project controller."""

    REQUEST_SERIALIZER = ProjectTemplateRequest()
    RESPONSE_SERIALIZER = ProjectTemplateResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a templates read manifest controller."""
        self.ctx = TemplatesCreateProjectCtrl.REQUEST_SERIALIZER.load({**user_data, **request_data}, unknown=EXCLUDE)
        super(TemplatesCreateProjectCtrl, self).__init__(cache, user_data, request_data)

        self.template = None

    @property
    def context(self):
        """Controller context."""
        return self.ctx

    @property
    def default_metadata(self):
        """Default metadata for project creation."""
        automated_update = True
        if self.template and "allow_template_update" in self.template:
            automated_update = self.template["allow_template_update"]

        metadata = {
            "__template_source__": self.ctx["git_url"],
            "__template_ref__": self.ctx["ref"],
            "__template_id__": self.ctx["identifier"],
            "__namespace__": self.ctx["project_namespace"],
            "__automated_update__": automated_update,
            "__repository__": self.ctx["project_repository"],
            "__sanitized_project_name__": self.ctx["project_name_stripped"],
            "__project_slug__": self.ctx["project_slug"],
        }

        return metadata

    @property
    def git_user(self):
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
            "name": self.ctx["project_name_stripped"],
            "fullname": self.ctx["fullname"],
            "email": self.ctx["email"],
            "owner": self.ctx["owner"],
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
        project = user_project_clone(self.user_data, self.ctx)
        templates = read_template_manifest(project.abs_path)
        self.template = next((template for template in templates if template["folder"] == self.ctx["identifier"]), None)
        if self.template is None:
            raise RenkuException("invalid identifier for target repository")

        repo = git.Repo(str(project.abs_path))
        self.template_version = repo.head.commit.hexsha

        # Verify missing parameters
        template_parameters = self.template.get("variables", {})
        provided_parameters = {p["key"]: p["value"] for p in self.ctx["parameters"]}
        missing_keys = list(template_parameters.keys() - provided_parameters.keys())
        if len(missing_keys) > 0:
            raise RenkuException(f"missing parameter: {missing_keys[0]}")

        return project, provided_parameters

    def new_project_push(self, project_path):
        """Push new project to the remote."""
        return new_repo_push(project_path, self.ctx["new_project_url_with_auth"])

    def new_project(self):
        """Create new project from template."""
        template_project, provided_parameters = self.setup_template()
        new_project = self.setup_new_project()
        new_project_path = new_project.abs_path

        source_path = template_project.abs_path / self.ctx["identifier"]

        with click_context(new_project_path, "create_from_template"):
            create_from_template_local(
                source_path,
                self.ctx["project_name"],
                provided_parameters,
                self.default_metadata,
                self.template_version,
                self.template.get("immutable_template_files", []),
                self.template.get("allow_template_update", False),
                self.git_user,
                self.ctx["url"],
                self.ctx["ref"],
                "service",
            )

        self.new_project_push(new_project_path)

        return {
            "url": self.ctx["new_project_url"],
            "namespace": self.ctx["project_namespace"],
            "name": self.ctx["project_name_stripped"],
        }

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(TemplatesCreateProjectCtrl.RESPONSE_SERIALIZER, self.new_project())
