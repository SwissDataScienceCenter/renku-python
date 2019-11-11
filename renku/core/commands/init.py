# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Project initialization logic."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import git

from renku.core.management.config import RENKU_HOME


def fetch_remote_template(
    url, folder, branch='master', tempdir=Path(tempfile.mkdtemp())
):
    """Fetch the remote template and checkout the relevant folder.

    Returns:
        The path with template files.

    """
    # clone the repo locally without checking out files
    template_repo = git.Repo.clone_from(
        url, tempdir, no_checkout=True, depth=1
    )

    # check branch
    template_branches = [branch.name for branch in template_repo.branches]
    if branch not in template_branches:
        raise ValueError(
            f'Branch "{branch}" doesn\'t exist in template "{url}"'
        )

    # check folder
    # TODO: update logic: look at `manifest.yaml` for templates metadata
    template_folders = [
        folder.name for folder in template_repo.heads[branch].commit.tree.trees
    ]
    if folder not in template_folders:
        raise ValueError(
            f'Folder "{folder}" doesn\'t exist in template "{url}"'
        )

    # checkout the specific folder
    template_repo.git.checkout(branch, tempdir / folder)

    return tempdir / folder


def validate_template(template_path):
    """Validate a local template."""
    # TODO: implement a better check
    required_folders = [RENKU_HOME]
    required_files = [f'{RENKU_HOME}/metadata.yml', 'Dockerfile']
    for folder in required_folders:
        if not Path(template_path, folder).is_dir():
            raise ValueError(
                f'Folder {folder} is required for the template to be valid'
            )
    for file in required_files:
        if not Path(template_path, file).is_file():
            raise ValueError(
                f'File {file} is required for the template to be valid'
            )
    return True


def create_from_template(
    template_path, client, name=None, description=None, force=None
):
    """Initialize a new project from a template."""
    # create empty repo
    client.init_empty_repository(force)

    # initialize with proper medata
    now = datetime.now(timezone.utc)
    metadata = {
        'name': name,
        'description': description,
        'date_created': now,
        'date_updated': now,
    }
    with client.commit():
        client.import_from_template(template_path, metadata, force)
