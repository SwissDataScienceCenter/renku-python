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
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.utils.git import clone_renku_repository


@inject.autoparams()
def _project_clone(
    url,
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
    path=None,
    install_githooks=True,
    skip_smudge=True,
    recursive=True,
    depth=None,
    progress=None,
    config=None,
    raise_git_except=False,
    checkout_revision=None,
    use_renku_credentials=False,
):
    """Clone Renku project repo, install Git hooks and LFS."""
    from renku.core.management.migrate import is_renku_project

    install_lfs = client_dispatcher.current_client.external_storage_requested

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

    client_dispatcher.push_client_to_stack(path=repository.path, external_storage_requested=install_lfs)
    database_dispatcher.push_database_to_stack(client_dispatcher.current_client.database_path)

    try:
        project_initialized = is_renku_project()
    finally:
        database_dispatcher.pop_database()
        client_dispatcher.pop_client()

    return repository, project_initialized


def project_clone_command():
    """Command to clone a renku project."""
    return Command().command(_project_clone).with_database()
