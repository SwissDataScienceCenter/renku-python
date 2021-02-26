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
from renku.core.commands.clone import project_clone
from renku.service.logger import service_log
from renku.service.views.decorators import requires_cache


@requires_cache
def user_project_clone(cache, user_data, project_data):
    """Clones the project for a given user."""
    if "project_id" in project_data:
        project_data.pop("project_id")

    user = cache.ensure_user(user_data)
    project = cache.make_project(user, project_data)
    project.abs_path.mkdir(parents=True, exist_ok=True)

    repo, project.initialized = project_clone(
        project_data["url_with_auth"],
        project.abs_path,
        depth=project_data["depth"] if project_data["depth"] != 0 else None,
        raise_git_except=True,
        config={"user.name": project_data["fullname"], "user.email": project_data["email"],},
        checkout_rev=project_data["ref"],
    )
    project.save()

    service_log.debug(f"project successfully cloned: {repo}")
    service_log.debug(f"project folder exists: {project.exists()}")

    return project
