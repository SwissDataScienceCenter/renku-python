# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Clone a Renku repo along with all Renku-specific initializations."""

import os
from pathlib import Path

from git import GitCommandError, Repo

from renku.core import errors
from renku.core.management.githooks import install
from renku.core.models.git import GitURL


def clone(
    url,
    path=None,
    install_githooks=True,
    install_lfs=True,
    skip_smudge=True,
    recursive=True,
    depth=None,
    progress=None,
    config=None,
    raise_git_except=False,
    checkout_rev=None,
):
    """Clone Renku project repo, install Git hooks and LFS."""
    from renku.core.management.client import LocalClient

    path = path or GitURL.parse(url).name

    if isinstance(path, Path):
        path = str(path)

    # Clone the project
    if skip_smudge:
        os.environ['GIT_LFS_SKIP_SMUDGE'] = '1'

    try:
        repo = Repo.clone_from(
            url, path, recursive=recursive, depth=depth, progress=progress
        )
    except GitCommandError as e:
        if not raise_git_except:
            raise errors.GitError(
                'Cannot clone remote Renku project: {}'.format(url)
            ) from e

        raise e

    remote_refs = [Path(ref.abspath).name for ref in repo.remote().refs]

    if checkout_rev in remote_refs:
        repo.git.checkout(checkout_rev)
    elif checkout_rev:
        repo.git.checkout(checkout_rev, b=checkout_rev)

    if config:
        config_writer = repo.config_writer()

        for key, value in config.items():
            key_path = key.split('.')
            key = key_path.pop()

            if not key_path or not key:
                raise errors.GitError(
                    'Cannot write to config. Section path or key is invalid.'
                )

            config_writer.set_value('.'.join(key_path), key, value)

        config_writer.release()

    client = LocalClient(path)

    if install_githooks:
        install(client=client, force=True)

    if install_lfs:
        command = ['git', 'lfs', 'install', '--local', '--force']
        if skip_smudge:
            command += ['--skip-smudge']
        try:
            repo.git.execute(command=command, with_exceptions=True)
        except GitCommandError as e:
            raise errors.GitError('Cannot install Git LFS') from e

    return repo
