# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Remove a file, a directory, or a symlink."""

import os
from pathlib import Path
from subprocess import run
from typing import List

from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.storage import check_external_storage, untrack_paths_from_storage
from renku.core.util import communication
from renku.core.util.git import get_git_user
from renku.core.util.os import delete_dataset_file, expand_directories
from renku.domain_model.project_context import project_context

try:
    from typing_extensions import Protocol, runtime_checkable  # NOTE: Required for Python 3.7 compatibility
except ImportError:
    from typing import Protocol, runtime_checkable  # type: ignore


@runtime_checkable
class EditCommandCallable(Protocol):
    """Typing Protocol for edit command."""

    def __call__(self, filename: str) -> None:
        """The call method."""
        ...


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _remove(sources: List[str], edit_command: EditCommandCallable, dataset_gateway: IDatasetGateway):
    """Remove files and check repository for potential problems.

    Args:
        sources(List[str]): Files to remove.
        edit_command(Callable[[str], None]): Command to execute for editing .gitattributes.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
    """
    repository = project_context.repository

    def get_relative_path(path):
        """Format path as relative to the project path."""
        abs_path = os.path.abspath(project_context.path / path)
        try:
            return str(Path(abs_path).relative_to(project_context.path))
        except ValueError:
            raise errors.ParameterError(f"File {abs_path} is not within the project.")

    files = {
        get_relative_path(source): get_relative_path(file_or_dir)
        for file_or_dir in sources
        for source in expand_directories((file_or_dir,))
    }

    # 1. Update dataset metadata files.
    progress_text = "Updating dataset metadata"
    all_datasets = dataset_gateway.get_all_active_datasets()
    communication.start_progress(progress_text, total=len(all_datasets))
    try:
        for dataset in all_datasets:
            remove = []
            for file in dataset.files:
                key = file.entity.path
                filepath = get_relative_path(key)
                if filepath in files:
                    remove.append(key)

            if remove:
                dataset = dataset.copy()
                for key in remove:
                    dataset.unlink_file(key)
                    delete_dataset_file(project_context.path / key, follow_symlinks=True)

                datasets_provenance = DatasetsProvenance()
                datasets_provenance.add_or_update(dataset, creator=get_git_user(repository))
            communication.update_progress(progress_text, amount=1)
    finally:
        communication.finalize_progress(progress_text)

    # 2. Manage .gitattributes for external storage.
    if check_external_storage():
        tracked = tuple(path for path, attr in repository.get_attributes(*files).items() if attr.get("filter") == "lfs")
        untrack_paths_from_storage(*tracked)
        existing = repository.get_attributes(*tracked)
        if existing:
            communication.warn("There are custom .gitattributes.\n")
            if communication.confirm('Do you want to edit ".gitattributes" now?'):
                edit_command(filename=str(project_context.path / ".gitattributes"))

    # Finally remove the files.
    files_to_remove = set(str(project_context.path / f) for f in files.values())
    final_sources = list(files_to_remove)
    if final_sources:
        run(["git", "rm", "-rf"] + final_sources, check=True)


def remove_command():
    """Command to remove a file."""
    return Command().command(_remove).require_clean().with_database(write=True).with_commit()
