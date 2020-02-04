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
import json
import os
import shutil
import time

from renku.service.cache import FileManagementCache, ProjectManagementCache
from renku.service.utils import make_file_path, make_project_path


def cache_files_cleanup():
    """Cache files a cleanup job."""
    ttl = int(os.getenv('RENKU_SVC_CLEANUP_TTL_FILES', 1800))
    cache = FileManagementCache()

    for file_key in cache.all_files_iter():
        try:
            user, _ = file_key.decode('utf-8').split('_')
        except ValueError:
            continue

        user = {'user_id': user}
        for file_meta in cache.hash_table(file_key).values():
            data = json.loads(file_meta)
            old = ((time.time() * 1e+3) - data['timestamp']) / 1e+3

            if old >= ttl:
                file_path = make_file_path(user, data)
                if file_path.exists() and file_path.is_file():
                    try:
                        file_path.unlink()
                    except FileNotFoundError:
                        pass

                if file_path.exists() and file_path.is_dir():
                    shutil.rmtree(str(file_path))

                cache.invalidate_file(user, data['file_id'])


def cache_project_cleanup():
    """Cache project a cleanup job."""
    ttl = int(os.getenv('RENKU_SVC_CLEANUP_TTL_PROJECTS', 1800))
    cache = ProjectManagementCache()

    for file_key in cache.all_projects_iter():
        try:
            user, _ = file_key.decode('utf-8').split('_')
        except ValueError:
            continue

        user = {'user_id': user}
        for file_meta in cache.hash_table(file_key).values():
            data = json.loads(file_meta)
            old = ((time.time() * 1e+3) - data['timestamp']) / 1e+3

            if old >= ttl:
                project_path = make_project_path(user, data)
                if project_path.exists():
                    shutil.rmtree(str(project_path))

                cache.invalidate_project(user, data['project_id'])
