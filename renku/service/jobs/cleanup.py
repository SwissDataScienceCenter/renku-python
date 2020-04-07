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
import os
import shutil
import time
from datetime import datetime

from renku.service.cache import ServiceCache
from renku.service.cache.models.job import USER_JOB_STATE_ENQUEUED, \
    USER_JOB_STATE_IN_PROGRESS


def file_cleanup_lock_check(jobs, file):
    """Check if file should be deleted."""
    for job in jobs:
        if file.file_id in job.locked:
            return True

    return False


def file_cleanup_ttl_check(file, ttl=None):
    """Check if file should be deleted."""
    ttl = ttl or int(os.getenv('RENKU_SVC_CLEANUP_TTL_FILES', 1800))

    # If record does not contain created_at,
    # it means its an old record, and we should mark it for deletion.
    created_at = (file.created_at -
                  datetime.utcfromtimestamp(0)).total_seconds() * 1e+3

    if not file.created_at:
        created_at = (time.time() - ttl) * 1e+3

    old = ((time.time() * 1e+3) - created_at) / 1e+3

    return old >= ttl and file.abs_path.exists()


def cache_files_cleanup():
    """Cache files a cleanup job."""
    cache = ServiceCache()

    for user, files in cache.user_files():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in
            [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for file in files:
            if file_cleanup_lock_check(jobs, file):
                continue

            ttl_check = file_cleanup_ttl_check(file)
            file_path = file.abs_path

            if ttl_check:
                if file_path.is_file():
                    try:
                        file_path.unlink()
                    except FileNotFoundError:
                        pass

                if file_path.is_dir():
                    shutil.rmtree(str(file_path))

                file.delete()


def project_cleanup_ttl_check(project, ttl=None):
    """Check if file should be deleted."""
    ttl = ttl or int(os.getenv('RENKU_SVC_CLEANUP_TTL_PROJECTS', 1800))
    # If record does not contain created_at,
    # it means its an old record, and we should mark it for deletion.
    created_at = (project.created_at -
                  datetime.utcfromtimestamp(0)).total_seconds() * 1e+3

    if not project.created_at:
        created_at = (time.time() - ttl) * 1e+3

    old = ((time.time() * 1e+3) - created_at) / 1e+3

    return old >= ttl and project.abs_path.exists()


def project_cleanup_lock_check(jobs, project):
    """Check if project should be deleted."""
    for job in jobs:
        if project.project_id in job.locked:
            return True

    return False


def cache_project_cleanup():
    """Cache project a cleanup job."""
    cache = ServiceCache()

    for user, projects in cache.user_projects():
        jobs = [
            job for job in cache.get_jobs(user) if job.state in
            [USER_JOB_STATE_ENQUEUED, USER_JOB_STATE_IN_PROGRESS]
        ]

        for project in projects:
            if project_cleanup_lock_check(jobs, project):
                continue

            if project_cleanup_ttl_check(project):
                shutil.rmtree(str(project.abs_path))

                project.delete()
