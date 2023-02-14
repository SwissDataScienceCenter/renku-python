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
"""Checks needed to determine integrity of the project."""

from renku.command.command_builder import inject
from renku.command.util import WARNING
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util import communication
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context


@inject.autoparams("project_gateway")
def check_project_id_group(fix, project_gateway: IProjectGateway, **_):
    """Check that projects in groups have the correct id set.

    Args:
        fix: Whether to fix found issues.
        project_gateway: Injected project gateway.
        _: keyword arguments.

    Returns:
        Tuple of whether project id is valid.
    """
    current_project = project_gateway.get_project()

    namespace, name = Project.get_namespace_and_name(
        remote=project_context.remote, repository=project_context.repository
    )

    if namespace is None or name is None:
        return True, None

    generated_id = Project.generate_id(namespace=namespace, name=name)

    if generated_id == current_project.id:
        return True, None

    if fix:
        communication.info(f"Fixing project id '{current_project.id}' -> '{generated_id}'")
        current_project.id = generated_id
        project_gateway.update_project(current_project)
        return True, None

    return True, (
        WARNING
        + "Project id doesn't match id created based on the current Git remote (use 'renku doctor --fix' to fix it):"
        f"\n\t'{current_project.id}' -> '{generated_id}'"
    )
