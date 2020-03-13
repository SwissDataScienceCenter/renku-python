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

from renku.service.cache import FileManagementCache, ProjectManagementCache


def cache_files_cleanup():
    """Cache files a cleanup job."""
    ttl = int(os.getenv('RENKU_SVC_CLEANUP_TTL_FILES', 1800))
    cache = FileManagementCache()

    for user, files in cache.user_files():
        for file in files:
            # If record does not contain created_at,
            # it means its an old record, and we should mark it for deletion.
            created_at = (file.created_at -
                          datetime.utcfromtimestamp(0)).total_seconds() * 1e+3

            if not file.created_at:
                created_at = (time.time() - ttl) * 1e+3

            old = ((time.time() * 1e+3) - created_at) / 1e+3

            if old >= ttl:
                file_path = file.abs_path
                if file_path.exists() and file_path.is_file():
                    try:
                        file_path.unlink()
                    except FileNotFoundError:
                        pass

                if file_path.exists() and file_path.is_dir():
                    shutil.rmtree(str(file_path))

                file.delete()


def cache_project_cleanup():
    """Cache project a cleanup job."""
    ttl = int(os.getenv('RENKU_SVC_CLEANUP_TTL_PROJECTS', 1800))
    cache = ProjectManagementCache()

    for user, projects in cache.user_projects():
        for project in projects:

            # If record does not contain created_at,
            # it means its an old record, and we should mark it for deletion.
            created_at = (project.created_at -
                          datetime.utcfromtimestamp(0)).total_seconds() * 1e+3

            if not project.created_at:
                created_at = (time.time() - ttl) * 1e+3

            old = ((time.time() * 1e+3) - created_at) / 1e+3

            if old >= ttl:
                project_path = project.abs_path
                if project_path.exists():
                    shutil.rmtree(str(project_path))

                project.delete()
