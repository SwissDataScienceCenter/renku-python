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
"""Dataset jobs."""
from git import GitCommandError, Repo
from urllib3.exceptions import HTTPError

from renku.core.commands.dataset import add_file, import_dataset
from renku.core.commands.save import repo_sync
from renku.core.errors import DatasetExistsError, ParameterError
from renku.core.utils.contexts import chdir
from renku.service.jobs.contexts import ProjectContext
from renku.service.views.decorators import requires_cache


@requires_cache
def dataset_import(
    cache,
    user,
    user_job_id,
    project_id,
    dataset_uri,
    short_name=None,
    extract=False,
    timeout=None,
):
    """Job for dataset import."""
    with ProjectContext(user, user_job_id, project_id) as ctx:
        try:
            ctx.user_job.in_progress()
            import_dataset(
                dataset_uri,
                short_name,
                extract,
                commit_message=f'service: dataset import {dataset_uri}'
            )
            _, remote_branch = repo_sync(
                Repo(ctx.project.abs_path), remote='origin'
            )
            ctx.user_job.update_extras('remote_branch', remote_branch)

            ctx.user_job.complete()
        except (HTTPError, ParameterError, DatasetExistsError) as exp:
            ctx.user_job.fail_job(str(exp))

            # Reraise exception, so we see trace in job metadata.
            raise exp


@requires_cache
def dataset_add_remote_file(
    cache, user, user_job_id, project_id, create_dataset, commit_message,
    short_name, url
):
    """Add a remote file to a specified dataset."""
    with ProjectContext(user, user_job_id, project_id) as ctx:
        try:
            ctx.user_job.in_progress()
            urls = url if isinstance(url, list) else [url]
            add_file(
                urls,
                short_name,
                create=create_dataset,
                commit_message=commit_message
            )

            _, remote_branch = repo_sync(
                Repo(ctx.project.abs_path), remote='origin'
            )
            ctx.user_job.update_extras('remote_branch', remote_branch)

            ctx.user_job.complete()
        except (HTTPError, BaseException, GitCommandError) as e:
            ctx.user_job.fail_job(str(e))
