# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

from renku.core import errors
from renku.core.incubation.command import Command
from renku.core.utils import communication


def _remove(client, sources, edit_command):
    """Remove files and check repository for potential problems."""
    from renku.core.management.git import _expand_directories

    def fmt_path(path):
        """Format path as relative to the client path."""
        abs_path = os.path.abspath(client.path / path)
        try:
            return str(Path(abs_path).relative_to(client.path))
        except ValueError:
            raise errors.ParameterError(f"File {abs_path} is not within the project.")

    files = {
        fmt_path(source): fmt_path(file_or_dir)
        for file_or_dir in sources
        for source in _expand_directories((file_or_dir,))
    }

    # 1. Update dataset metadata files.
    progress_text = "Updating dataset metadata"
    communication.start_progress(progress_text, total=len(client.datasets))
    try:
        for dataset in client.datasets.values():
            remove = []
            for file_ in dataset.files:
                key = file_.path
                filepath = fmt_path(file_.path)
                if filepath in files:
                    remove.append(key)

            if remove:
                for key in remove:
                    dataset.unlink_file(key)
                    client.remove_file(client.path / key)

                dataset.to_yaml()
            communication.update_progress(progress_text, amount=1)
    finally:
        communication.finalize_progress(progress_text)

    # 2. Manage .gitattributes for external storage.
    if client.check_external_storage():
        tracked = tuple(path for path, attr in client.find_attr(*files).items() if attr.get("filter") == "lfs")
        client.untrack_paths_from_storage(*tracked)
        existing = client.find_attr(*tracked)
        if existing:
            communication.warn("There are custom .gitattributes.\n")
            if communication.confirm('Do you want to edit ".gitattributes" now?'):
                edit_command(filename=str(client.path / ".gitattributes"))

    # Finally remove the files.
    files_to_remove = set(str(client.path / f) for f in files.values())
    final_sources = list(files_to_remove)
    if final_sources:
        run(["git", "rm", "-rf"] + final_sources, check=True)


def remove_command():
    """Command to remove a file."""
    return Command().command(_remove).require_clean().with_commit()
