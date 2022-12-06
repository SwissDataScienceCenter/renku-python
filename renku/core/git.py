# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Git repository management."""

import os
import sys
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Tuple, Type

from renku.core import errors
from renku.core.storage import checkout_paths_from_storage
from renku.core.util.contexts import Isolation
from renku.core.util.git import get_dirty_paths
from renku.core.util.os import get_absolute_path
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository


@contextmanager
def with_worktree(path=None, branch_name=None, commit=None, merge_args=("--ff-only",)):
    """Create new worktree."""
    from renku.infrastructure.repository import NULL_TREE

    delete = branch_name is None
    new_branch = commit is not NULL_TREE

    _, isolation, path, branch_name = prepare_worktree(path=path, branch_name=branch_name, commit=commit)
    try:
        yield
    except (Exception, BaseException) as e:
        finalize_worktree(
            isolation=isolation,
            path=path,
            branch_name=branch_name,
            delete=delete,
            new_branch=new_branch,
            merge_args=merge_args,
            exception=e,
        )
        raise
    else:
        finalize_worktree(
            isolation=isolation,
            path=path,
            branch_name=branch_name,
            delete=delete,
            new_branch=new_branch,
            merge_args=merge_args,
        )


def prepare_worktree(path=None, branch_name=None, commit=None) -> Tuple[Repository, Isolation, Path, str]:
    """Set up a Git worktree to provide isolation."""
    from renku.infrastructure.repository import NULL_TREE

    original_repository = project_context.repository
    original_path = project_context.path

    path = path or tempfile.mkdtemp()
    branch_name = branch_name or f"renku/run/isolation/{uuid.uuid4().hex}"

    project_context.push_path(path)
    path = project_context.path

    # TODO sys.argv
    if commit is NULL_TREE:
        original_repository.create_worktree(path=path, reference="HEAD", detach=True)
        repository = project_context.repository
        repository.run_git_command("checkout", "--orphan", branch_name)
        repository.remove("*", recursive=True, force=True)
    else:
        revision = None
        if commit:
            revision = commit.hexsha
        original_repository.create_worktree(path=path, branch=branch_name, reference=revision)
        repository = project_context.repository

    repository.get_configuration = original_repository.get_configuration  # type: ignore

    # Keep current directory relative to repository root.
    relative = Path(os.path.relpath(Path(".").resolve(), original_path))

    # Reroute standard streams
    original_mapped_std = get_mapped_std_streams(original_repository.all_files)
    mapped_std = {}
    for name, stream in original_mapped_std.items():
        stream_path = Path(path) / (Path(stream).relative_to(original_path))
        stream_path = stream_path.absolute()

        if not stream_path.exists():
            stream_path.parent.mkdir(parents=True, exist_ok=True)
            stream_path.touch()

        mapped_std[name] = stream_path

    _clean_streams(original_repository, original_mapped_std)

    new_cwd = Path(path) / relative
    new_cwd.mkdir(parents=True, exist_ok=True)

    isolation = Isolation(cwd=str(new_cwd), **mapped_std)
    isolation.__enter__()

    return repository, isolation, path, branch_name


def finalize_worktree(isolation, path, branch_name, delete, new_branch, merge_args=("--ff-only",), exception=None):
    """Cleanup and merge a previously created Git worktree."""
    exc_info: Tuple[Optional[Type[Any]], Any, Any] = (None, None, None)

    if exception:
        exc_info = (type(exception), exception, exception.__traceback__)

    isolation.__exit__(*exc_info)

    project_context.pop_context()

    repository = project_context.repository

    try:
        repository.run_git_command("merge", branch_name, *merge_args)
    except errors.GitCommandError:
        raise errors.FailedMerge(repository, branch_name, merge_args)

    if delete:
        repository.remove_worktree(path)

        if new_branch:
            # delete the created temporary branch
            repository.branches.remove(branch_name)

    if project_context.external_storage_requested:
        checkout_paths_from_storage()


def get_mapped_std_streams(lookup_paths, streams=("stdin", "stdout", "stderr")):
    """Get a mapping of standard streams to given paths."""
    # FIXME add device number too
    standard_inos = {}
    for stream in streams:
        try:
            stream_stat = os.fstat(getattr(sys, stream).fileno())
            key = stream_stat.st_dev, stream_stat.st_ino
            standard_inos[key] = stream
        except Exception:  # FIXME UnsupportedOperation
            pass
        # FIXME if not getattr(sys, stream).istty()

    def stream_inos(paths):
        """Yield tuples with stats and path."""
        for path in paths:
            try:
                stat = os.stat(path)
                key = (stat.st_dev, stat.st_ino)
                if key in standard_inos:
                    yield standard_inos[key], path
            except FileNotFoundError:  # pragma: no cover
                pass

        return []

    return dict(stream_inos(lookup_paths)) if standard_inos else {}


def _clean_streams(repository, mapped_streams):
    """Clean mapped standard streams."""
    for stream_name in ("stdout", "stderr"):
        stream = mapped_streams.get(stream_name)
        if not stream:
            continue

        absolute_path = get_absolute_path(stream, repository.path)
        path = os.path.relpath(absolute_path, start=repository.path)
        if path not in repository.files:
            os.remove(absolute_path)
        else:
            checksum = repository.get_object_hash(path=absolute_path, revision="HEAD")
            repository.copy_content_to_file(path=absolute_path, checksum=checksum, output_path=path)


def ensure_clean(ignore_std_streams=False):
    """Make sure the repository is clean."""
    repository = project_context.repository

    dirty_paths = get_dirty_paths(repository)
    mapped_streams = get_mapped_std_streams(dirty_paths)

    if ignore_std_streams:
        if dirty_paths - set(mapped_streams.values()):
            _clean_streams(repository, mapped_streams)
            raise errors.DirtyRepository(repository)
    elif repository.is_dirty():
        _clean_streams(repository, mapped_streams)
        raise errors.DirtyRepository(repository)
