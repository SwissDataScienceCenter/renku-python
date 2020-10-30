# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""API Project context."""

from pathlib import Path

from git import GitError, Repo
from werkzeug.local import LocalStack

from renku.core.management.config import RENKU_HOME


class Project:
    """API Project context class."""

    _project_contexts = LocalStack()

    def __init__(self):
        self._client = None

    def __enter__(self):
        self._client = _get_local_client()

        self._project_contexts.push(self)

    def __exit__(self, type, value, traceback):
        project_context = self._project_contexts.pop()
        if project_context is not self:
            raise RuntimeError("Project context was changed.")

    @property
    def client(self):
        """Return the LocalClient instance."""
        return self._client


def get_current_project():
    """Return current Project object if any."""
    return Project._project_contexts.top


def _get_local_client():
    from renku.core.management.client import LocalClient

    try:
        repo = Repo(".", search_parent_directories=True)
    except GitError:
        pass
    else:
        project_root = Path(repo.git_dir).parent
        if (project_root / RENKU_HOME).exists():
            return LocalClient(project_root)
