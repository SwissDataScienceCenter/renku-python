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
"""Move or rename a file, a directory, or a symlink."""

import os
from pathlib import Path
from typing import List, Optional

from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.dataset.dataset import move_files
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.storage import track_paths_in_storage, untrack_paths_from_storage
from renku.core.util import communication
from renku.core.util.metadata import is_protected_path
from renku.core.util.os import get_relative_path, is_subpath
from renku.domain_model.project_context import project_context


def move_command():
    """Command to move or rename a file, a directory, or a symlink."""
    return Command().command(_move).require_migration().require_clean().with_database(write=True).with_commit()


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _move(sources: List[str], destination: str, force: bool, verbose: bool, to_dataset: Optional[str]):
    """Move files and check repository for potential problems.

    Args:
        sources(List[str]): Source file(s) to move.
        destination(str): Destination to move files to.
        force(bool): Whether or not to overwrite destination files.
        verbose(bool): Toggle verbose output.
        to_dataset(Optional[str]): Target dataset to move files into.
    """
    repository = project_context.repository

    absolute_destination = _get_absolute_path(destination)
    absolute_sources = [_get_absolute_path(src) for src in sources]

    if to_dataset:
        target_dataset = DatasetsProvenance().get_by_name(to_dataset, strict=True)
        if not is_subpath(absolute_destination, _get_absolute_path(target_dataset.get_datadir())):
            raise errors.ParameterError(
                f"Destination {destination} must be in {target_dataset.get_datadir()} when moving to a dataset."
            )

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
        _warn_about_dataset_files(files)

    # NOTE: we don't check if source and destination are the same or if multiple sources are moved to the same
    # destination; git mv will check those and we raise if git mv fails.

    _warn_about_ignored_destinations(files.values())

    if not absolute_destination.exists() and not absolute_destination.is_symlink():
        if is_rename:
            absolute_destination.parent.mkdir(parents=True, exist_ok=True)
        else:
            absolute_destination.mkdir(parents=True, exist_ok=True)

    try:
        repository.move(*sources, destination=destination, force=force)
    except errors.GitCommandError as e:
        raise errors.OperationError(f"Git operation failed: {e}")

    # Handle symlinks
    for src, dst in files.items():
        if dst.is_symlink():
            target = src.parent / os.readlink(dst)
            dst.unlink()
            Path(dst).symlink_to(os.path.relpath(target, start=os.path.dirname(dst)))

    files_to_untrack = (str(src.relative_to(project_context.path)) for src in files)
    untrack_paths_from_storage(*files_to_untrack)
    # NOTE: Warn about filter after untracking from LFS to avoid warning about LFS filters
    _warn_about_git_filters(files)
    track_paths_in_storage(*[dst for dst in files.values() if not dst.is_dir()])

    # NOTE: Force-add to include possible ignored files
    repository.add(*files.values(), force=True)

    move_files(files=files, to_dataset_name=to_dataset)

    if verbose:
        _show_moved_files(project_context.path, files)


def _traverse_path(path):
    """Recursively yield all files and directories within a path.

    Args:
        path: Root path.

    Returns:
        Iterator of all paths in root path.
    """
    path = Path(path)

    if path.is_dir():
        yield from path.rglob("*")
    else:
        yield path


def _get_dst(path, src_root, dst_root, is_rename):
    parent = "." if is_rename else src_root.name
    return dst_root / parent / os.path.relpath(path, start=src_root)


def _get_absolute_path(path):
    """Resolve path and raise if path is outside the repo or is protected.

    Args:
        path: Path to make absolute.

    Returns:
        Absolute path.
    """
    abs_path = Path(os.path.abspath(path))

    if is_protected_path(abs_path):
        raise errors.ParameterError(f"Path '{path}' is protected.")

    try:
        abs_path.relative_to(project_context.path)
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

    existing_str = "\n\t".join(existing)
    raise errors.ParameterError(
        f"The following move target exist, use '--force' flag to overwrite them:\n\t{existing_str}"
    )


def _warn_about_ignored_destinations(destinations):
    ignored = project_context.repository.get_ignored_paths(*destinations)
    if ignored:
        ignored_str = "\n\t".join((str(Path(p).relative_to(project_context.path)) for p in ignored))
        communication.warn(f"The following moved path match .gitignore:\n\t{ignored_str}")


def _warn_about_git_filters(files):
    """Check if there are any git attributes for files including LFS.

    Args:
        files: Files to check.
    """
    repository = project_context.repository

    src_attrs = []
    dst_attrs = []

    for path, attrs in repository.get_attributes(*files).items():
        src = Path(path)
        dst = files[src].relative_to(project_context.path)
        src = src.relative_to(project_context.path)
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
        src_attrs_str = "\n\t".join(src_attrs)
        dst_attrs_str = "\n\t".join(dst_attrs)
        communication.warn(
            f"There are custom git attributes for the following files:\n\t{src_attrs_str}\n"
            f"You need to edit '.gitattributes' and add the following:\n\t{dst_attrs_str}"
        )


@inject.autoparams()
def _warn_about_dataset_files(files, dataset_gateway: IDatasetGateway):
    """Check if any of the files are part of a dataset.

    Args:
        files: Files to check.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
    """
    found = []
    for dataset in dataset_gateway.get_all_active_datasets():
        for src, dst in files.items():
            relative_src = get_relative_path(src, project_context.path)
            if not relative_src:
                continue

            found_file = dataset.find_file(relative_src)
            if not found_file:
                continue
            if not found_file.is_external and not is_subpath(dst, project_context.path / dataset.get_datadir()):
                found.append(str(src))

    if not found:
        return

    found_str = "\n\t".join(found)
    communication.confirm(
        msg="You are trying to move dataset files out of a datasets data directory. "
        f"These files will be removed from the source dataset:\n\t{found_str}",
        abort=True,
        warning=True,
    )


def _show_moved_files(project_path, files):
    for path in sorted(files):
        src = path.relative_to(project_path)
        dst = files[path].relative_to(project_path)
        communication.echo(f"{src} -> {dst}")
