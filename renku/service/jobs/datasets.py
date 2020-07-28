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
from renku.core.management.datasets import DownloadProgressCallback
from renku.core.utils.contexts import chdir
from renku.service.cache.serializers.job import JobSchema
from renku.service.views.decorators import requires_cache


class DatasetImportJobProcess(DownloadProgressCallback):
    """Track dataset import job progress."""

    schema = JobSchema()

    def __init__(self, cache, job):
        """Construct dataset import job progress."""
        self.cache = cache
        self.job = job

        super().__init__(None, None)

    def __call__(self, description, total_size):
        """Job progress call."""
        self.job.extras = {
            'description': description,
            'total_size': total_size,
        }

        super().__init__(description, total_size)
        return self

    def update(self, size):
        """Update status."""
        self.job.extras['progress_size'] = size
        self.job.save()

    def finalize(self):
        """Finalize job tracking."""
        self.job.complete()


@requires_cache
def dataset_import(
    cache,
    user,
    user_job_id,
    project_id,
    dataset_uri,
    name=None,
    extract=False,
    timeout=None,
):
    """Job for dataset import."""
    user = cache.ensure_user(user)
    user_job = cache.get_job(user, user_job_id)
    project = cache.get_project(user, project_id)

    with chdir(project.abs_path):
        try:
            user_job.in_progress()

            import_dataset(
                dataset_uri,
                name,
                extract,
                commit_message=f'service: dataset import {dataset_uri}',
                progress=DatasetImportJobProcess(cache, user_job)
            )

            _, remote_branch = repo_sync(
                Repo(project.abs_path), remote='origin'
            )
            user_job.update_extras('remote_branch', remote_branch)

            user_job.complete()
        except (
            HTTPError, ParameterError, DatasetExistsError, GitCommandError
        ) as exp:
            user_job.fail_job(str(exp))

            # Reraise exception, so we see trace in job metadata.
            raise exp


@requires_cache
def dataset_add_remote_file(
    cache, user, user_job_id, project_id, create_dataset, commit_message, name,
    url
):
    """Add a remote file to a specified dataset."""
    user = cache.ensure_user(user)
    user_job = cache.get_job(user, user_job_id)
    project = cache.get_project(user, project_id)

    try:
        user_job.in_progress()

        with chdir(project.abs_path):
            urls = url if isinstance(url, list) else [url]
            add_file(
                urls,
                name,
                create=create_dataset,
                commit_message=commit_message
            )

            _, remote_branch = repo_sync(
                Repo(project.abs_path), remote='origin'
            )
            user_job.update_extras('remote_branch', remote_branch)

            user_job.complete()
    except (HTTPError, BaseException, GitCommandError) as e:
        user_job.fail_job(str(e))
