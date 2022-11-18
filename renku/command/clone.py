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

from pathlib import Path
from typing import Any, Dict, Optional, Union

from git.remote import RemoteProgress
from pydantic import validate_arguments

from renku.command.command_builder.command import Command
from renku.domain_model.project_context import project_context


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _project_clone(
    url: str,
    path: Optional[Union[str, Path]] = None,
    install_githooks: bool = True,
    install_mergetool: bool = True,
    skip_smudge: bool = True,
    recursive: bool = True,
    depth: Optional[int] = None,
    progress: Optional[RemoteProgress] = None,
    config: Optional[Dict[str, Any]] = None,
    raise_git_except: bool = False,
    checkout_revision: Optional[str] = None,
    use_renku_credentials: bool = False,
):
    """Clone Renku project repo, install Git hooks and LFS.

    Args:
        url(str): Git URL to clone.
        path(Optional[str]): Path to clone to (Default value = None).
        install_githooks(bool): Whether to install the pre-commit hook or not (Default value = True).
        install_mergetool(bool): Whether to install the renku metadata git mergetool or not (Default value = True).
        skip_smudge(bool): Whether to skip pulling files from LFS (Default value = True).
        recursive(bool): Recursively clone (Default value = True).
        depth(Optional[int]): Clone depth (commits from HEAD) (Default value = None).
        progress(Optional[RemoteProgress]): Git progress object (Default value = None).
        config(Optional[Dict[str, Any]]): Initial config (Default value = None).
        raise_git_except(bool): Whether to raise Git exceptions or not (Default value = False).
        checkout_revision(Optional[str]): Specific revision to check out (Default value = None).
        use_renku_credentials(bool): Whether to use credentials stored in renku (Default value = False).

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
