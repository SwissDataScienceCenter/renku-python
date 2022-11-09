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
import urllib

from urllib3.exceptions import HTTPError

from renku.command.dataset import add_to_dataset_command, import_dataset_command
from renku.core import errors
from renku.core.util.contexts import renku_project_context
from renku.core.util.git import push_changes
from renku.domain_model.git import GitURL
from renku.infrastructure.repository import Repository
from renku.ui.service.logger import worker_log
from renku.ui.service.utils.callback import ServiceCallback
from renku.ui.service.views.decorators import requires_cache


@requires_cache
def dataset_import(
    cache,
    user,
    user_job_id,
    project_id,
    dataset_uri,
    name=None,
    extract=False,
    tag=None,
    timeout=None,
    commit_message=None,
    data_directory=None,
):
    """Job for dataset import."""
    user = cache.ensure_user(user)
    worker_log.debug(f"executing dataset import job for {user.user_id}:{user.fullname}")

    user_job = cache.get_job(user, user_job_id)
    user_job.in_progress()

    try:
        worker_log.debug(f"retrieving metadata for project {project_id}")
        project = cache.get_project(user, project_id)
        with renku_project_context(project.abs_path):
            worker_log.debug(f"project found in cache - importing dataset {dataset_uri}")
            communicator = ServiceCallback(user_job=user_job)

            gitlab_token = user.token if _is_safe_to_pass_gitlab_token(project.git_url, dataset_uri) else None

            command = import_dataset_command().with_commit_message(commit_message)
            command.with_communicator(communicator).build().execute(
                uri=dataset_uri,
                name=name,
                extract=extract,
                tag=tag,
                yes=True,
                gitlab_token=gitlab_token,
                datadir=data_directory,
            )

            worker_log.debug("operation successful - syncing with remote")
            remote_branch = push_changes(Repository(project.abs_path), remote="origin")
            user_job.update_extras("remote_branch", remote_branch)

            user_job.complete()
            worker_log.debug("job completed")
    except (HTTPError, errors.ParameterError, errors.RenkuException, errors.GitCommandError) as exp:
        user_job.fail_job(str(exp))

        # Reraise exception, so we see trace in job metadata
        # and in metrics as failed job.
        raise exp


def _is_safe_to_pass_gitlab_token(project_git_url, dataset_uri):
    """Passing token is safe if project and dataset belong to the same deployment."""
    project_host = GitURL.parse(project_git_url).hostname
    dataset_host = urllib.parse.urlparse(dataset_uri).netloc

    # NOTE: URLs changed from domain/gitlab to gitlab.domain when moving to cloud native gitlab
    if project_host.startswith("gitlab.") and not dataset_host.startswith("gitlab."):
        project_host = project_host.replace("gitlab.", "", 1)

    return project_host == dataset_host


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

        with renku_project_context(project.abs_path):
            urls = url if isinstance(url, list) else [url]

            worker_log.debug(f"adding files {urls} to dataset {name}")
            command = add_to_dataset_command().with_commit_message(commit_message).build()
            result = command.execute(dataset_name=name, urls=urls, create=create_dataset)
            if result.error:
                raise result.error

            worker_log.debug("operation successful - syncing with remote")
            remote_branch = push_changes(Repository(project.abs_path), remote="origin")
            user_job.update_extras("remote_branch", remote_branch)

            user_job.complete()
            worker_log.debug("job completed")
    except (HTTPError, BaseException, errors.GitCommandError, errors.RenkuException) as exp:
        user_job.fail_job(str(exp))

        # Reraise exception, so we see trace in job metadata
        # and in metrics as failed job.
        raise exp
