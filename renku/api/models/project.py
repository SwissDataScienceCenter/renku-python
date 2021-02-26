# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
unless you want to have access to Project metadata (e.g. path). To separate
parts of your script that uses Renku entities, you can create a Project context
manager and interact with Renku inside it:

.. code-block:: python

    from renku.api import Project, Input

    with Project():
        input_1 = Input("data_1")

"""
from functools import wraps

from git import GitError, Repo
from werkzeug.local import LocalStack


class Project:
    """API Project context class."""

    _project_contexts = LocalStack()

    def __init__(self):
        self._client = _get_local_client()

    def __enter__(self):
        self._project_contexts.push(self)

        return self

    def __exit__(self, type, value, traceback):
        project_context = self._project_contexts.pop()
        if project_context is not self:
            raise RuntimeError("Project context was changed.")

    @property
    def client(self):
        """Return the LocalClient instance."""
        return self._client

    @property
    def path(self):
        """Absolute path to project's root directory."""
        return self._client.path.resolve()


def ensure_project_context(fn):
    """Check existence of a project context."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        project = _get_current_project() or Project()
        return fn(*args, **kwargs, project=project)

    return wrapper


def _get_current_project():
    """Return current project context if any or a new project object."""
    return Project._project_contexts.top if Project._project_contexts.top else None


def _get_local_client():
    from renku.core.management.client import LocalClient

    try:
        repo = Repo(".", search_parent_directories=True)
    except GitError:
        path = "."
    else:
        path = repo.working_dir

    return LocalClient(path)
