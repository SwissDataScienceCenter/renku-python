# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
r"""Renku API Project.

Project class acts as a context for other Renku entities like Dataset, or
Inputs/Outputs. It provides access to internals of a Renku project for such
entities.

Normally, you do not need to create an instance of Project class directly
unless you want to have access to Project metadata (e.g. path) or get its status. To separate
parts of your script that uses Renku entities, you can create a Project context
manager and interact with Renku inside it:

.. code-block:: python

    from renku.api import Project, Input

    with Project():
        input_1 = Input("input_1", "path_1")

You can use Project's ``status`` method to get info about outdated outputs and
activities, and modified or deleted inputs:

.. code-block:: python

    from renku.api import Project

    outdated_generations, outdated_activities, modified_inputs, deleted_inputs = Project().status()

"""

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from werkzeug.local import LocalStack

from renku.command.status import get_status_command
from renku.core.util.git import get_git_repository
from renku.core.workflow.run import StatusResult
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.infrastructure.repository import Repository


class Project:
    """API Project context class."""

    _project_contexts: LocalStack = LocalStack()

    def __init__(self):
        try:
            repository = get_git_repository()
            path = repository.path
        except ValueError:
            repository = None
            path = Path(".")

        project_context.replace_path(path)
        project_context.repository = repository  # type: ignore
        self._path = project_context.path
        self._repository = repository

    def __enter__(self):
        self._project_contexts.push(self)

        return self

    def __exit__(self, type, value, traceback):
        context = self._project_contexts.pop()
        if context is not self:
            raise RuntimeError("Project context was changed.")

    @property
    def repository(self) -> Optional["Repository"]:
        """Return the Repository instance."""
        return self._repository

    @property
    def path(self) -> Path:
        """Absolute path to project's root directory."""
        return self._path

    def status(self, paths: Optional[List[Union[Path, str]]] = None, ignore_deleted: bool = False) -> StatusResult:
        """Return status of a project.

        Args:
            paths(Optional[List[Union[Path, str]]]): Limit the status to this list of paths (Default value = None).
            ignore_deleted(bool): Whether to ignore deleted generations.

        Returns:
            StatusResult: Status of the project.

        """
        return get_status_command().build().execute(paths=paths, ignore_deleted=ignore_deleted).output
