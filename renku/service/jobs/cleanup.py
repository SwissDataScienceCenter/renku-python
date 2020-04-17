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
"""Cleanup jobs."""
from renku.service.cache import ServiceCache
from renku.service.cache.models.job import USER_JOB_STATE_ENQUEUED, \
    USER_JOB_STATE_IN_PROGRESS


def cache_files_cleanup():
    """Cache files a cleanup job."""
    cache = ServiceCache()

    for user, files in cache.user_files():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in
            [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for file in files:
            if file.is_locked(jobs):
                continue

            if file.ttl_expired():
                file.purge()


def cache_project_cleanup():
    """Cache project a cleanup job."""
    cache = ServiceCache()

    for user, projects in cache.user_projects():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in
            [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for project in projects:
            if project.is_locked(jobs):
                continue

            if project.ttl_expired():
                project.purge()
