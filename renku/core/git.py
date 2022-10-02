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
"""Wrap Git functionality."""

import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.storage import checkout_paths_from_storage
from renku.core.util.contexts import Isolation
from renku.core.util.git import get_dirty_paths
from renku.core.util.os import get_absolute_path
from renku.core.util.urls import remove_credentials
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository

COMMIT_DIFF_STRATEGY = "DIFF"
STARTED_AT = int(time.time() * 1e3)


@contextmanager
@inject.autoparams("project_gateway", "database_gateway")
def with_project_metadata(
    project_gateway: IProjectGateway,
    database_gateway: IDatabaseGateway,
    read_only: bool = False,
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    description: Optional[str] = None,
    keywords: Optional[List[str]] = None,
    custom_metadata: Optional[Dict] = None,
):
    """Yield an editable metadata object.

    Args:
        project_gateway(IProjectGateway): Injected project gateway.
        database_gateway(IDatabaseGateway): Injected database gateway.
        read_only(bool): Whether to save changes or not (Default value = False).
        name(Optional[str]): Name of the project (when creating a new one) (Default value = None).
        namespace(Optional[str]): Namespace of the project (when creating a new one) (Default value = None).
        description(Optional[str]): Project description (when creating a new one) (Default value = None).
        keywords(Optional[List[str]]): Keywords for the project (when creating a new one) (Default value = None).
        custom_metadata(Optional[Dict]): Custom JSON-LD metadata (when creating a new project)
            (Default value = None).
    """
    try:
        project = project_gateway.get_project()
    except ValueError:
        project = Project.from_path(
            name=name, namespace=namespace, description=description, keywords=keywords, custom_metadata=custom_metadata
        )

    yield project

    if not read_only:
        project_gateway.update_project(project)
        database_gateway.commit()


@contextmanager
def commit(
    commit_only=None,
    commit_empty=True,
    raise_if_empty=False,
    commit_message=None,
    abbreviate_message=True,
    skip_dirty_checks=False,
):
    """Automatic commit."""
    diff_before = prepare_commit(commit_only=commit_only, skip_dirty_checks=skip_dirty_checks)

    yield

    finalize_commit(
        diff_before=diff_before,
        commit_only=commit_only,
        commit_empty=commit_empty,
        raise_if_empty=raise_if_empty,
        commit_message=commit_message,
        abbreviate_message=abbreviate_message,
    )


@contextmanager
def worktree(path=None, branch_name=None, commit=None, merge_args=("--ff-only",)):
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


def prepare_commit(commit_only=None, skip_dirty_checks=False, skip_staging: bool = False):
    """Gather information about repo needed for committing later on."""
    repository = project_context.repository

    def ensure_not_untracked(path):
        """Ensure that path is not part of git untracked files."""
        for file_path in repository.untracked_files:
            is_parent = (project_context.path / file_path).parent == (project_context.path / path)
            is_equal = str(path) == file_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(repository)

    def ensure_not_staged(path):
        """Ensure that path is not part of git staged files."""
        path = str(path)
        for file_path in repository.staged_changes:
            is_parent = str(file_path.a_path).startswith(path)
            is_equal = path == file_path.a_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(repository)

    if skip_staging:
        if not isinstance(commit_only, list) or len(commit_only) == 0:
            raise errors.OperationError("Cannot use ``skip_staging`` without specifying files to commit.")

    diff_before = set()

    if commit_only == COMMIT_DIFF_STRATEGY:
        if len(repository.staged_changes) > 0 or len(repository.unstaged_changes) > 0:
            repository.reset()

        # Exclude files created by pipes.
        diff_before = {
            file for file in repository.untracked_files if STARTED_AT - int(Path(file).stat().st_ctime * 1e3) >= 1e3
        }

    if isinstance(commit_only, list) and not skip_dirty_checks:
        for path in commit_only:
            ensure_not_untracked(path)
            ensure_not_staged(path)

    return diff_before


def finalize_commit(
    diff_before,
    commit_only=None,
    commit_empty=True,
    raise_if_empty=False,
    commit_message=None,
    abbreviate_message=True,
    skip_staging: bool = False,
):
    """Commit modified/added paths."""
    from renku.core.util.git import shorten_message
    from renku.infrastructure.repository import Actor
    from renku.version import __version__, version_url

    committer = Actor(name=f"renku {__version__}", email=version_url)
    repository = project_context.repository

    change_types = {item.a_path: item.change_type for item in repository.unstaged_changes}

    if commit_only == COMMIT_DIFF_STRATEGY:
        # Get diff generated in command.
        staged_after = set(change_types.keys())

        modified_after_change_types = {item.a_path: item.change_type for item in repository.staged_changes}

        modified_after = set(modified_after_change_types.keys())

        change_types.update(modified_after_change_types)

        diff_after = set(repository.untracked_files).union(staged_after).union(modified_after)

        # Remove files not touched in command.
        commit_only = list(diff_after - diff_before)

    if isinstance(commit_only, list):
        for path_ in commit_only:
            p = project_context.path / path_
            if p.exists() or change_types.get(str(path_)) == "D":
                repository.add(path_)

    if not commit_only:
        repository.add(all=True)

    try:
        diffs = [d.a_path for d in repository.staged_changes]
    except errors.GitError:
        diffs = []

    if not commit_empty and not diffs:
        if raise_if_empty:
            raise errors.NothingToCommit()
        return

    if commit_message and not isinstance(commit_message, str):
        raise errors.CommitMessageEmpty()

    elif not commit_message:
        argv = [os.path.basename(sys.argv[0])] + [remove_credentials(arg) for arg in sys.argv[1:]]

        commit_message = " ".join(argv)

    if abbreviate_message:
        commit_message = shorten_message(commit_message)

    # NOTE: Only commit specified paths when skipping staging area
    paths = commit_only if skip_staging else []
    # Ignore pre-commit hooks since we have already done everything.
    repository.commit(commit_message + project_context.transaction_id, committer=committer, no_verify=True, paths=paths)


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
    exc_info = (None, None, None)

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
