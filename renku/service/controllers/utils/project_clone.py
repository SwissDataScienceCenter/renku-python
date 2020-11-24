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
"""Utilities for renku service controllers."""
import shutil

from renku.core.commands.clone import project_clone
from renku.core.errors import RenkuException
from renku.service.logger import service_log
from renku.service.utils import make_project_path
from renku.service.views.decorators import requires_cache


@requires_cache
def user_project_clone(cache, user_data, project_data):
    """Clones the project for a given user."""
    local_path = make_project_path(user_data, project_data)
    if local_path is None:
        raise RenkuException("project not found")

    user = cache.ensure_user(user_data)

    if local_path.exists():
        shutil.rmtree(local_path)

        for project in cache.get_projects(user):
            if project.abs_path == local_path:
                project.delete()

        if "project_id" in project_data:
            project_data.pop("project_id")

    repo, initialized = project_clone(
        project_data["url_with_auth"],
        local_path,
        depth=project_data["depth"] if project_data["depth"] != 0 else None,
        raise_git_except=True,
        config={"user.name": project_data["fullname"], "user.email": project_data["email"],},
        checkout_rev=project_data["ref"],
    )
    project_data["initialized"] = initialized

    service_log.debug(f"project successfully cloned: {repo}")
    service_log.debug(f"project folder exists: {local_path.exists()}")

    project = cache.make_project(user, project_data)
    return project
