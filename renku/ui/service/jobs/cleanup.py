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
import shutil

from renku.ui.service.cache import ServiceCache
from renku.ui.service.cache.models.job import USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS
from renku.ui.service.logger import worker_log


def cache_files_cleanup():
    """Cache files a cleanup job."""
    cache = ServiceCache()
    worker_log.debug("executing cache files cleanup")

    for user, files in cache.user_files():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for file in files:
            if file.is_locked(jobs):
                continue

            if file.exists() and file.ttl_expired():
                worker_log.debug(f"purging file {file.file_id}:{file.file_name}")
                file.purge()
            elif not file.exists():
                file.delete()

    for user, chunks in cache.user_chunks():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        chunk_folders = set()

        for chunk in chunks:
            if chunk.exists() and chunk.ttl_expired():
                worker_log.debug(f"purging chunk {chunk.chunk_file_id}:{chunk.file_name}")
                chunk.purge()
                chunk_folders.add(chunk.abs_path.parent)
            elif not chunk.exists():
                chunk.delete()
                chunk_folders.add(chunk.abs_path.parent)

        for chunk_folder in chunk_folders:
            shutil.rmtree(chunk_folder, ignore_errors=True)


def cache_project_cleanup():
    """Cache project a cleanup job."""
    cache = ServiceCache()
    worker_log.debug("executing cache projects cleanup")

    for user, projects in cache.user_projects():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for project in projects:
            if project.is_locked(jobs):
                continue

            if project.exists() and project.ttl_expired():
                worker_log.debug(f"purging project {project.project_id}:{project.name}")
                project.purge()
            elif not project.exists():
                project.delete()
