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

from renku.command.clone import project_clone_command
from renku.core.util.contexts import renku_project_context
from renku.ui.service.cache.models.project import Project
from renku.ui.service.logger import service_log
from renku.ui.service.views.decorators import requires_cache


@requires_cache
def user_project_clone(cache, user_data, project_data):
    """Clones the project for a given user."""
    if "project_id" in project_data:
        project_data.pop("project_id")

    user = cache.ensure_user(user_data)
    project = cache.make_project(user, project_data, persist=False)

    # NOTE: Create parent dir so lock file can be created.
    project.abs_path.parent.mkdir(parents=True, exist_ok=True)

    with project.write_lock(), renku_project_context(project.abs_path, check_git_path=False):
        git_url = project_data.get("git_url")

        if git_url is not None:
            try:
                # NOTE: If two requests ran at the same time, by the time we acquire the lock a project might already
                # be cloned by an earlier request.
                found_project = Project.get(
                    (Project.user_id == user_data["user_id"])
                    & (Project.git_url == git_url)
                    & (Project.project_id != project.project_id)
                )
            except ValueError:
                pass
            else:
                service_log.debug(f"project already cloned, skipping clone: {git_url}")
                return found_project

        if project.abs_path.exists():
            # NOTE: Remove dir since a previous clone might have failed somewhere in the middle.
            shutil.rmtree(str(project.abs_path))

        project.abs_path.mkdir(parents=True, exist_ok=True)

        repo, project.initialized = (
            project_clone_command()
            .build()
            .execute(
                project_data["url_with_auth"],
                path=project.abs_path,
                depth=project_data["depth"],
                raise_git_except=True,
                config={
                    "user.name": project_data["fullname"],
                    "user.email": project_data["email"],
                    "pull.rebase": False,
                },
                checkout_revision=project_data["ref"],
            )
        ).output
        project.save()

    service_log.debug(f"project successfully cloned: {repo}")

    return project
