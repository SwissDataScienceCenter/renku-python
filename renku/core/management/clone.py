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

from pathlib import Path
from typing import Tuple

from renku.core import errors
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.metadata.repository import Repository
from renku.core.models.git import GitURL


def _handle_git_exception(e, raise_git_except, progress):
    """Handle git exceptions."""
    if not raise_git_except:
        lines = progress.other_lines + progress.error_lines if progress else []
        error = "".join([f"\n\t{line}" for line in lines if line.strip()])
        message = f"Cannot clone remote Renku project: Git exited with code {e.status} and error message:\n {error}"
        raise errors.GitError(message)


@inject.autoparams()
def clone(
    url,
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
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
) -> Tuple[Repository, bool]:
    """Clone Renku project repo, install Git hooks and LFS."""
    from renku.core.management.githooks import install
    from renku.core.management.migrate import is_renku_project

    assert config is None or isinstance(config, dict), f"Config should be a dict not '{type(config)}'"

    path = path or GitURL.parse(url).name

    if isinstance(path, Path):
        path = str(path)

    # Clone the project
    env = {"GIT_LFS_SKIP_SMUDGE": "1"} if skip_smudge else None

    try:
        # NOTE: Try to clone, assuming checkout_rev is a branch (if it is set)
        repository = Repository.clone_from(
            url, path, branch=checkout_rev, recursive=recursive, depth=depth, progress=progress, env=env
        )
    except errors.GitCommandError as e:
        # NOTE: clone without branch set, in case checkout_rev was not a branch but a tag or commit
        if not checkout_rev:
            _handle_git_exception(e, raise_git_except, progress)
            raise

        try:
            repository = Repository.clone_from(url, path, recursive=recursive, depth=depth, progress=progress)
        except errors.GitCommandError as e:
            _handle_git_exception(e, raise_git_except, progress)
            raise

        try:
            # NOTE: Now that we cloned successfully, try to checkout the checkout_rev
            repository.checkout(checkout_rev)
        except errors.GitCommandError as e:
            msg = str(e)
            if "is not a commit and a branch" in msg and "cannot be created from it" in msg:
                return repository, False  # NOTE: Project has no commits to check out

            raise

    try:
        _ = repository.head.commit
    except errors.GitError:
        # NOTE: git repo has no head commit, which means it is empty/not a renku project
        return repository, False

    if config:
        with repository.configuration(writable=True) as config_writer:
            for key, value in config.items():
                try:
                    section, option = key.rsplit(".", maxsplit=1)
                except ValueError:
                    raise errors.GitError(f"Cannot write to config: Invalid config '{key}'")

                config_writer.set_value(section, option, value)

    client_dispatcher.push_client_to_stack(path=path, external_storage_requested=install_lfs)
    database_dispatcher.push_database_to_stack(client_dispatcher.current_client.database_path)

    try:
        if install_githooks:
            install(force=True)

        if install_lfs:
            command = ["lfs", "install", "--local", "--force"]
            if skip_smudge:
                command += ["--skip-smudge"]
            try:
                repository.run_git_command(*command)
            except errors.GitCommandError as e:
                raise errors.GitError("Cannot install Git LFS") from e

        project_initialized = is_renku_project()
    finally:
        database_dispatcher.pop_database()
        client_dispatcher.pop_client()

    return repository, project_initialized
