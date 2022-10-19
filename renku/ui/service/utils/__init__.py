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

from renku.core.util.git import push_changes
from renku.ui.service.config import CACHE_PROJECTS_PATH, CACHE_UPLOADS_PATH


def make_project_path(user, project):
    """Construct full path for cached project."""
    valid_user = user and "user_id" in user
    valid_project = project and "owner" in project and "name" in project and "project_id" in project

    if valid_user and valid_project:
        return CACHE_PROJECTS_PATH / user["user_id"] / project["owner"] / project["slug"]


def make_file_path(user, cached_file):
    """Construct full path for cache file."""
    valid_user = user and "user_id" in user
    valid_file = cached_file and "file_name" in cached_file

    if valid_user and valid_file:
        return CACHE_UPLOADS_PATH / user["user_id"] / cached_file["relative_path"]


def valid_file(user, cached_file):
    """Ensure file system and cache state matches."""
    file_path = make_file_path(user, cached_file)

    if file_path.exists():
        cached_file["is_dir"] = file_path.is_dir()
        return cached_file


def new_repo_push(repo_path, source_url, source_name="origin", source_branch="master"):
    """Push a new repo to origin."""
    from renku.infrastructure.repository import Repository

    repository = Repository(repo_path)
    repository.remotes.add(source_name, source_url)
    branch = push_changes(repository, remote=source_name)
    return branch == source_branch
