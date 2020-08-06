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
"""Project related jobs."""

from git import GitCommandError, Repo
from urllib3.exceptions import HTTPError

from renku.core.commands.migrate import migrate_project
from renku.core.commands.save import repo_sync
from renku.core.errors import ParameterError, RenkuException
from renku.core.utils.contexts import chdir
from renku.service.views.decorators import requires_cache


def execute_migration(project):
    """Execute project migrations."""
    messages = []

    def collect_message(msg):
        """Collect migration message."""
        messages.append(msg)

    with chdir(project.abs_path):
        was_migrated = migrate_project(progress_callback=collect_message)

    return messages, was_migrated


@requires_cache
def migrate_job(cache, user_data, project_id, user_job_id):
    """Execute migrations job."""
    user = cache.ensure_user(user_data)
    user_job = cache.get_job(user, user_job_id)

    try:
        project = cache.get_project(user, project_id)
        messages, was_migrated = execute_migration(project)

        user_job.update_extras("messages", messages)
        user_job.update_extras("was_migrated", was_migrated)

        _, remote_branch = repo_sync(Repo(project.abs_path), remote="origin")
        user_job.update_extras("remote_branch", remote_branch)

        user_job.complete()
    except (HTTPError, ParameterError, GitCommandError, RenkuException) as exp:
        user_job.update_extras("error", str(exp))
        user_job.fail_job()
        return
