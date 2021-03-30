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
"""Move or rename a file, a directory, or a symlink."""

import os
from pathlib import Path

import git

from renku.core import errors
from renku.core.incubation.command import Command
from renku.core.utils import communication
from renku.core.utils.git import add_to_git


def move_command():
    """Command to move or rename a file, a directory, or a symlink."""
    return Command().command(_move).require_migration().require_clean().with_commit()


def _move(client, sources, destination, force, verbose, to_dataset):
    """Move files and check repository for potential problems."""
    if to_dataset:
        client.load_dataset(to_dataset, strict=True)

    absolute_destination = _get_absolute_path(client, destination)
    absolute_sources = [_get_absolute_path(client, src) for src in sources]

    is_rename = len(absolute_sources) == 1 and (
        not absolute_destination.exists() or (absolute_destination.is_file() and absolute_sources[0].is_file())
    )

    files = {
        path: _get_dst(path, src, absolute_destination, is_rename)
        for src in absolute_sources
        for path in _traverse_path(src)
        if not path.is_dir()
    }

    if not files:
        raise errors.ParameterError("There are no files to move.")
    if not force:
        _check_existing_destinations(files.values())

    # NOTE: we don't check if source and destination are the same or if multiple sources are moved to the same
    # destination; git mv will check those and we raise if git mv fails.

    _warn_about_ignored_destinations(client, files.values())

    if not absolute_destination.exists() and not absolute_destination.is_symlink():
        if is_rename:
            absolute_destination.parent.mkdir(parents=True, exist_ok=True)
        else:
            absolute_destination.mkdir(parents=True, exist_ok=True)

    try:
        client.repo.git.mv([str(src) for src in sources] + [str(destination)], force=force)
    except git.GitError as e:
        raise errors.OperationError(f"Git operation failed: {e}")

    # Handle symlinks
    for src, dst in files.items():
        if dst.is_symlink():
            target = src.parent / os.readlink(dst)
            dst.unlink()
            Path(dst).symlink_to(os.path.relpath(target, start=os.path.dirname(dst)))

    files_to_untrack = (str(src.relative_to(client.path)) for src in files)
    client.untrack_paths_from_storage(*files_to_untrack)
    # NOTE: Warn about filter after untracking from LFS to avoid warning about LFS filters
    _warn_about_git_filters(client, files)
    client.track_paths_in_storage(*[dst for dst in files.values() if not dst.is_dir()])

    # NOTE: Force-add to include possible ignored files
    add_to_git(client.repo.git, *files.values(), force=True)
    commit = client.repo.index.commit(f"renku mv: committing {len(files)} moved files")

    client.move_files(files=files, to_dataset=to_dataset, commit=commit)

    if verbose:
        _show_moved_files(client, files)


def _traverse_path(path):
    """Recursively yield all files and directories within a path."""
    path = Path(path)

    if path.is_dir():
        yield from path.rglob("*")
    else:
        yield path


def _get_dst(path, src_root, dst_root, is_rename):
    parent = "." if is_rename else src_root.name
    return dst_root / parent / os.path.relpath(path, start=src_root)


def _get_absolute_path(client, path):
    """Resolve path and raise if path is outside the repo or is protected."""
    abs_path = Path(os.path.abspath(path))

    if client.is_protected_path(abs_path):
        raise errors.ParameterError(f"Path '{path}' is protected.")

    try:
        abs_path.relative_to(client.path)
    except ValueError:
        raise errors.ParameterError(f"Path '{path}' is outside the project.")

    return abs_path


def _check_existing_destinations(destinations):
    existing = set()
    for dst in destinations:
        if dst.exists() or dst.is_symlink():
            existing.add(str(dst.relative_to(os.getcwd())))

    if not existing:
        return

    existing = "\n\t".join(existing)
    raise errors.ParameterError(f"The following move target exist, use '--force' flag to overwrite them:\n\t{existing}")


def _warn_about_ignored_destinations(client, destinations):
    ignored = client.find_ignored_paths(*destinations)
    if ignored:
        ignored = "\n\t".join((str(Path(p).relative_to(client.path)) for p in ignored))
        communication.warn(f"The following moved path match .gitignore:\n\t{ignored}")


def _warn_about_git_filters(client, files):
    """Check if there are any git attributes for files including LFS."""
    src_attrs = []
    dst_attrs = []

    for path, attrs in client.find_attr(*files).items():
        src = Path(path)
        dst = files[src].relative_to(client.path)
        src = src.relative_to(client.path)
        attrs_text = ""
        for name, value in attrs.items():
            if value == "unset":
                attrs_text += f" -{name}"
            elif value == "set":
                attrs_text += f" {name}"
            else:
                attrs_text += f" {name}={value}"

        src_attrs.append(f"{str(src)}{attrs_text}")
        dst_attrs.append(f"{str(dst)}{attrs_text}")

    if src_attrs:
        src_attrs = "\n\t".join(src_attrs)
        dst_attrs = "\n\t".join(dst_attrs)
        communication.warn(
            f"There are custom git attributes for the following files:\n\t{src_attrs}\n"
            f"You need to edit '.gitattributes' and add the following:\n\t{dst_attrs}"
        )


def _show_moved_files(client, files):
    for path in sorted(files):
        src = path.relative_to(client.path)
        dst = files[path].relative_to(client.path)
        communication.echo(f"{src} -> {dst}")
