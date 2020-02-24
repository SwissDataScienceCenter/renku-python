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
from urllib3.exceptions import HTTPError

from renku.core.commands.dataset import import_dataset
from renku.core.errors import ParameterError
from renku.core.management.datasets import DownloadProgressCallback
from renku.core.utils.contexts import chdir
from renku.service.jobs.constants import USER_JOB_STATE_COMPLETED, \
    USER_JOB_STATE_FAILED, USER_JOB_STATE_IN_PROGRESS
from renku.service.serializers.jobs import UserJob
from renku.service.utils import make_project_path, repo_sync
from renku.service.views.decorators import requires_cache


def fail_job(cache, user_job, user, error):
    """Mark job as failed."""
    user_job['state'] = USER_JOB_STATE_FAILED
    user_job['extras']['error'] = error
    cache.set_job(user, user_job)


class DatasetImportJobProcess(DownloadProgressCallback):
    """Track dataset import job progress."""

    schema = UserJob()

    def __init__(self, cache, user, job):
        """Construct dataset import job progress."""
        self.cache = cache
        self.user = user
        self.job = job

        super().__init__(None, None)

    def __call__(self, description, total_size):
        """Job progress call."""
        self.job['state'] = USER_JOB_STATE_IN_PROGRESS
        self.job['extras'] = {
            'description': description,
            'total_size': total_size,
        }

        self.cache.set_job(self.user, self.job)
        super().__init__(description, total_size)

        return self

    def update(self, size):
        """Update status."""
        self.job['extras']['progress_size'] = size
        self.cache.set_job(self.user, self.job)

    def finalize(self):
        """Finalize job tracking."""
        self.job['state'] = USER_JOB_STATE_COMPLETED
        self.cache.set_job(self.user, self.job)


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
    user_job = cache.get_job(user, user_job_id)
    project = cache.get_project(user, project_id)
    project_path = make_project_path(user, project)

    with chdir(project_path):
        try:
            import_dataset(
                dataset_uri,
                short_name,
                extract,
                progress=DatasetImportJobProcess(cache, user, user_job)
            )
        except (HTTPError, ParameterError) as exp:
            fail_job(cache, user_job, user, str(exp))

            # Reraise exception, so we see trace in job metadata.
            raise exp

    if not repo_sync(project_path):
        error = 'failed to push refs'
        fail_job(cache, user_job, user, error)

        raise RuntimeError(error)
