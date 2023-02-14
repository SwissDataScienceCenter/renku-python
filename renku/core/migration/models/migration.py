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
"""Migration utility functions."""

import os
from collections import defaultdict
from enum import IntFlag
from typing import Any, Dict, NamedTuple, Optional

from renku.core import errors
from renku.core.constant import RENKU_HOME
from renku.core.util import communication
from renku.infrastructure.repository import Repository, git_unicode_unescape


class MigrationType(IntFlag):
    """Type of migration that is being executed."""

    DATASETS = 1
    WORKFLOWS = 2
    STRUCTURAL = 4
    ALL = DATASETS | WORKFLOWS | STRUCTURAL


class MigrationOptions(NamedTuple):
    """Migration options."""

    strict: bool
    preserve_identifiers: bool
    type: MigrationType = MigrationType.ALL


class MigrationContext:
    """Context containing required migration information."""

    def __init__(self, strict: bool, type: MigrationType, preserve_identifiers: bool):
        self.options = MigrationOptions(strict=strict, type=type, preserve_identifiers=preserve_identifiers)
        self.dataset_migration_context: Optional[DatasetMigrationContext] = None
        self.cache: Optional[RepositoryCache] = None
        self.cwl_cache: Optional[Dict[str, Any]] = None
        self.plan_cache: Optional[Dict[str, Any]] = None


class DatasetMigrationContext:
    """The context to get path info in a specific revision."""

    def __init__(self, repository, revision):
        self.repository = repository
        self.revision = revision

    def exists(self, path) -> bool:
        """If a given path existed at the revision."""
        try:
            self.repository.run_git_command("cat-file", "-e", f"{self.revision}:{path}")
        except errors.GitCommandError:
            return False
        else:
            return True

    def is_dir(self, path) -> bool:
        """If a given path was a directory at the revision."""
        try:
            result = self.repository.run_git_command("cat-file", "-t", f"{self.revision}:{path}")
        except errors.GitCommandError:
            return False
        else:
            return "tree" in result

    def get_previous_commit(self, path):
        """Get previous commit of a given path starting from the revision."""
        return self.repository.get_previous_commit(path, revision=self.revision)


class RepositoryCache:
    """Cache for a git repository."""

    def __init__(self, repository: "Repository", cache, cwl_files_commits):
        self.repository = repository
        self.cache = cache
        self.cwl_files_commits = cwl_files_commits

    @classmethod
    def from_repository(cls, repository: "Repository"):
        """Return a cached repository."""
        cache = defaultdict(list)
        cwl_files_commits_map = {}

        for n, commit in enumerate(repository.iterate_commits(full_history=True), start=1):
            communication.echo(f"Caching commit {n}", end="\r")

            cwl_files = []
            for file in commit.get_changes():
                # Ignore deleted files
                if file.deleted:
                    continue

                path = file.b_path
                cache[path].append(commit)

                if path.startswith(f"{RENKU_HOME}/workflow/") and path.endswith(".cwl"):
                    cwl_files.append(os.path.realpath(repository.path / path))

            cls._update_cwl_files_and_commits(commit, cwl_files_commits_map, cwl_files)

        communication.echo(40 * " ", end="\r")

        return RepositoryCache(repository, cache, cwl_files_commits_map)

    @staticmethod
    def _update_cwl_files_and_commits(commit, cwl_files_commits_map, cwl_files):
        if len(cwl_files) != 1:
            return

        path = cwl_files[0]
        existing_commit = cwl_files_commits_map.get(path)

        if existing_commit is None:
            cwl_files_commits_map[path] = commit
        elif existing_commit.compare_to(commit) < 0:  # existing commit is older
            cwl_files_commits_map[path] = commit

    def find_previous_commit(self, path, revision="HEAD"):
        """Return a previous commit for a given path starting from 'revision'."""

        def find_from_repository(path, revision):
            try:
                return self.repository.get_previous_commit(path=path, revision=revision, full_history=True)
            except errors.GitCommitNotFoundError:
                communication.warn(f"Cannot find previous commit for {path} from {str(revision)}")
                return revision

        try:
            path = (self.repository.path / path).relative_to(self.repository.path)
        except ValueError:
            pass
        path = str(path)

        if revision == "HEAD":
            revision = self.repository.head.commit

        commits = self.cache.get(git_unicode_unescape(path))
        if not commits:
            return find_from_repository(path, revision)

        if revision in commits:
            return revision

        for commit in commits:
            if commit.compare_to(revision) <= 0:
                return commit

        # No commit was found
        return find_from_repository(path, revision)
