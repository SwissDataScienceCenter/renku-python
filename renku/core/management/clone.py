# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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


def _handle_git_exception(e, raise_git_except, progress):
    """Handle git exceptions."""
    if not raise_git_except:
        lines = progress.other_lines + progress.error_lines if progress else []
        error = "".join([f"\n\t{line}" for line in lines if line.strip()])
        message = f"Cannot clone remote Renku project: Git exited with code {e.status} and error message:\n {error}"
        raise errors.GitError(message)

    raise e


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
        os.environ["GIT_LFS_SKIP_SMUDGE"] = "1"

    clone_config = None
    if isinstance(config, str):
        clone_config = config
        config = None

    try:
        # NOTE: Try to clone, assuming checkout_rev is a branch (if it is set)
        repo = Repo.clone_from(
            url, path, branch=checkout_rev, recursive=recursive, depth=depth, progress=progress, config=clone_config
        )
    except GitCommandError as e:
        # NOTE: clone without branch set, in case checkout_rev was not a branch but a tag or commit
        if not checkout_rev:
            _handle_git_exception(e, raise_git_except, progress)

        try:
            repo = Repo.clone_from(url, path, recursive=recursive, depth=depth, progress=progress, config=clone_config)
        except GitCommandError as e:
            _handle_git_exception(e, raise_git_except, progress)

        try:
            # NOTE: Now that we cloned successfully, try to checkout the checkout_rev
            repo.git.checkout(checkout_rev)
        except GitCommandError as e:
            msg = str(e)
            if "is not a commit and a branch" in msg and "cannot be created from it" in msg:
                return repo, False  # NOTE: Project has no commits to check out

            raise

    try:
        repo.head.commit
    except ValueError:
        # NOTE: git repo has no head commit, which means it is empty/not a renku project
        return repo, False

    if config:
        config_writer = repo.config_writer()

        for key, value in config.items():
            key_path = key.split(".")
            key = key_path.pop()

            if not key_path or not key:
                raise errors.GitError("Cannot write to config. Section path or key is invalid.")

            config_writer.set_value(".".join(key_path), key, value)

        config_writer.release()

    client = LocalClient(path)

    if install_githooks:
        install(client=client, force=True)

    if install_lfs:
        command = ["git", "lfs", "install", "--local", "--force"]
        if skip_smudge:
            command += ["--skip-smudge"]
        try:
            repo.git.execute(command=command, with_exceptions=True)
        except GitCommandError as e:
            raise errors.GitError("Cannot install Git LFS") from e

    return repo, bool(client.project)
