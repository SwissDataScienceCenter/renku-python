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
import yaml

from renku.core.management.config import RENKU_HOME

TEMPLATE_MANIFEST = 'manifest.yaml'


def fetch_template(source, ref='master', tempdir=None):
    """Fetch the template and checkout the relevant data.

    :param source: url or full path of the templates repository
    :param ref: reference for git checkout - branch, commit or tag
    :param tempdir: temporary work directory path
    :return: template manifest Path
    """
    if tempdir is None:
        tempdir = Path(tempfile.mkdtemp())

    # clone the repo locally without checking out files
    template_repo = git.Repo.clone_from(source, tempdir, no_checkout=True)

    # fetch ref and set the HEAD
    template_repo.remotes.origin.fetch(ref)
    try:
        template_repo.head.reset(template_repo.commit(ref))
    except git.exc.BadName:
        ref = f'origin/{ref}'
        template_repo.head.reset(template_repo.commit(ref))
    git_repo = git.Git(tempdir)

    # checkout the manifest
    git_repo.checkout(TEMPLATE_MANIFEST)
    return tempdir / TEMPLATE_MANIFEST


def validate_template(template_path):
    """Validate a local template.

    :param template_path: path of the template to validate
    """
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


def validate_template_manifest(manifest):
    """Validate manifet content.

    :param manifest: manifest file content
    """
    if not isinstance(manifest, list):
        raise ValueError((
            f'The repository doesn\'t contain a valid',
            f'"{TEMPLATE_MANIFEST}" file'
        ))
    for template in manifest:
        if not template['name']:
            raise ValueError((
                f'Every template listed in "{TEMPLATE_MANIFEST}"',
                f' must have a name'
            ))
        for attribute in ['folder', 'description']:
            if not template[attribute]:
                raise ValueError((
                    f'Template "{template["name"]}" doesn\'t ',
                    f'have a {attribute} attribute'
                ))
    return True


def read_template_manifest(folder, checkout=False):
    """Extract template metadata from the manifest file.

    :param folder: path where to find the template manifest file
    :param checkout: checkout the template folder from local repo
    """
    manifest_path = folder / TEMPLATE_MANIFEST

    with manifest_path.open('r') as fp:
        manifest = yaml.safe_load(fp)
        validate_template_manifest(manifest)

        if checkout:
            git_repo = git.Git(folder)
            template_folders = [template['folder'] for template in manifest]
            for template_folder in template_folders:
                template_path = folder / template_folder
                git_repo.checkout(template_path)
                validate_template(template_path)

        return manifest


def create_from_template(
    template_path, client, name=None, description=None, force=None
):
    """Initialize a new project from a template."""
    # create empty repo
    client.init_repository(force)

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
