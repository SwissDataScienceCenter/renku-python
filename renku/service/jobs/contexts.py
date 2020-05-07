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
"""Context for jobs."""
import contextlib
import os
import time
from pathlib import Path

from redis import BusyLoadingError

from renku.core.utils.contexts import chdir
from renku.service.jobs.queues import WorkerQueues
from renku.service.utils.communication import ServiceCallback, communication
from renku.service.views.decorators import requires_cache


@contextlib.contextmanager
def enqueue_retry(queue, retry=3):
    """Ensure job gets queued."""
    count = 0
    while count < retry:
        try:
            yield WorkerQueues.get(queue)
        except (OSError, IOError, BusyLoadingError):
            time.sleep(2**count)
            count += 1
        break


class ProjectContext(object):
    """Context manager for project.

    Sets up project, user, userjob and callback communication.
    """

    @requires_cache
    def __init__(self, cache, user, project_id, user_job_id):
        """Create the context manager."""
        self.user = cache.ensure_user(user)
        self.user_job = cache.get_job(user, user_job_id)
        self.project = cache.get_project(user, project_id)
        self.chdir = chdir(self.project.abs_path)
        self.callback = ServiceCallback(self.user_job)

    def __enter__(self):
        """Change directory and setup communication."""
        self.chdir.__enter__()
        communication.subscribe(self.callback)
        return self

    def __exit__(self, kind, value, tb):
        """Change back directory and unsubscribe communication."""
        self.chdir.__exit__(kind, value, tb)

        communication.unsubscribe(self.callback)
