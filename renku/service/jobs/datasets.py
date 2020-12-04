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

from renku.core.commands.dataset import add_to_dataset, import_dataset
from renku.core.commands.save import repo_sync
from renku.core.errors import ParameterError, RenkuException
from renku.core.utils.contexts import click_context
from renku.service.logger import worker_log
from renku.service.utils.callback import ServiceCallback
from renku.service.views.decorators import requires_cache


@requires_cache
def dataset_import(
    cache, user, user_job_id, project_id, dataset_uri, name=None, extract=False, timeout=None,
):
    """Job for dataset import."""
    user = cache.ensure_user(user)
    worker_log.debug(f"executing dataset import job for {user.user_id}:{user.fullname}")

    user_job = cache.get_job(user, user_job_id)
    user_job.in_progress()

    try:
        worker_log.debug(f"retrieving metadata for project {project_id}")
        project = cache.get_project(user, project_id)
        with click_context(project.abs_path, "dataset_import"):
            worker_log.debug(f"project found in cache - importing dataset {dataset_uri}")
            communicator = ServiceCallback(user_job=user_job)

            command = import_dataset().with_commit_message(f"service: dataset import {dataset_uri}")
            command.with_communicator(communicator).build().execute(dataset_uri, name, extract, yes=True)

            worker_log.debug("operation successful - syncing with remote")
            _, remote_branch = repo_sync(Repo(project.abs_path), remote="origin")
            user_job.update_extras("remote_branch", remote_branch)

            user_job.complete()
            worker_log.debug("job completed")
    except (HTTPError, ParameterError, RenkuException, GitCommandError) as exp:
        user_job.fail_job(str(exp))

        # Reraise exception, so we see trace in job metadata
        # and in metrics as failed job.
        raise exp


@requires_cache
def dataset_add_remote_file(cache, user, user_job_id, project_id, create_dataset, commit_message, name, url):
    """Add a remote file to a specified dataset."""
    user = cache.ensure_user(user)
    worker_log.debug(f"executing dataset add remote file job for {user.user_id}:{user.fullname}")

    user_job = cache.get_job(user, user_job_id)
    user_job.in_progress()

    try:
        worker_log.debug(f"checking metadata for project {project_id}")
        project = cache.get_project(user, project_id)

        with click_context(project.abs_path, "dataset_add_remote_file"):
            urls = url if isinstance(url, list) else [url]

            worker_log.debug(f"adding files {urls} to dataset {name}")
            command = add_to_dataset().with_commit_message(commit_message).build()
            result = command.execute(urls=urls, name=name, create=create_dataset)
            if result.error:
                raise result.error

            worker_log.debug("operation successful - syncing with remote")
            _, remote_branch = repo_sync(Repo(project.abs_path), remote="origin")
            user_job.update_extras("remote_branch", remote_branch)

            user_job.complete()
            worker_log.debug("job completed")
    except (HTTPError, BaseException, GitCommandError, RenkuException) as exp:
        user_job.fail_job(str(exp))

        # Reraise exception, so we see trace in job metadata
        # and in metrics as failed job.
        raise exp
