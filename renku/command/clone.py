# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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

from renku.command.command_builder.command import Command
from renku.domain_model.project_context import project_context


def _project_clone(
    url,
    path=None,
    install_githooks=True,
    install_mergetool=True,
    skip_smudge=True,
    recursive=True,
    depth=None,
    progress=None,
    config=None,
    raise_git_except=False,
    checkout_revision=None,
    use_renku_credentials=False,
):
    """Clone Renku project repo, install Git hooks and LFS.

    Args:
        url: Git URL to clone.
        path: Path to clone to (Default value = None).
        install_githooks: Whether to install the pre-commit hook or not (Default value = True).
        install_mergetool: Whether to install the renku metadata git mergetool or not (Default value = True).
        skip_smudge: Whether to skip pulling files from LFS (Default value = True).
        recursive: Recursively clone (Default value = True).
        depth: Clone depth (commits from HEAD) (Default value = None).
        progress: Git progress object (Default value = None).
        config: Initial config (Default value = None).
        raise_git_except: Whether to raise Git exceptions or not (Default value = False).
        checkout_revision: Specific revision to check out (Default value = None).
        use_renku_credentials: Whether to use credentials stored in renku (Default value = False).

    Returns:
        Tuple of cloned ``Repository`` and whether it's a Renku project or not.
    """
    from renku.command.mergetool import setup_mergetool
    from renku.core.migration.migrate import is_renku_project
    from renku.core.util.git import clone_renku_repository

    install_lfs = project_context.external_storage_requested

    repository = clone_renku_repository(
        url=url,
        path=path,
        install_githooks=install_githooks,
        install_lfs=install_lfs,
        skip_smudge=skip_smudge,
        recursive=recursive,
        depth=depth,
        progress=progress,
        config=config,
        raise_git_except=raise_git_except,
        checkout_revision=checkout_revision,
        use_renku_credentials=use_renku_credentials,
    )

    with project_context.with_path(repository.path):
        project_initialized = is_renku_project()

        if project_initialized and install_mergetool:
            setup_mergetool(with_attributes=False)

    return repository, project_initialized


def project_clone_command():
    """Command to clone a renku project."""
    return Command().command(_project_clone).with_database()
