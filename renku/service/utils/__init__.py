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
"""Renku service utility functions."""
import uuid

from git import GitCommandError, Repo

from renku.service.config import CACHE_PROJECTS_PATH, CACHE_UPLOADS_PATH


def make_project_path(user, project):
    """Construct full path for cached project."""
    valid_user = user and 'user_id' in user
    valid_project = project and 'owner' in project and 'name' in project

    if valid_user and valid_project:
        return (
            CACHE_PROJECTS_PATH / user['user_id'] / project['owner'] /
            project['name']
        )


def make_file_path(user, cached_file):
    """Construct full path for cache file."""
    valid_user = user and 'user_id' in user
    valid_file = cached_file and 'file_name' in cached_file

    if valid_user and valid_file:
        return (
            CACHE_UPLOADS_PATH / user['user_id'] / cached_file['relative_path']
        )


def valid_file(user, cached_file):
    """Ensure file system and cache state matches."""
    file_path = make_file_path(user, cached_file)

    if file_path.exists():
        cached_file['is_dir'] = file_path.is_dir()
        return cached_file


def repo_sync(repo_path, remote_names=('origin', )):
    """Sync the repo with the remotes."""
    repo = Repo(repo_path)
    pushed_branch = None

    for remote in repo.remotes:
        if remote.name in remote_names:
            try:
                repo.git.push(remote.name, repo.active_branch)
                pushed_branch = repo.active_branch
            except GitCommandError as e:
                if 'protected branches' not in e.stderr:
                    raise e

                pushed_branch = uuid.uuid4().hex
                repo.git.checkout(b=pushed_branch)
                repo.git.push(remote.name, repo.active_branch)

    return pushed_branch
