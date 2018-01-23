# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Renga repository."""

import datetime
import os

from contextlib import contextmanager

import click
import filelock
import yaml
from dulwich.repo import Repo as GitRepo

from ._config import RENGA_HOME, read_config, write_config
from ._git import get_git_home

try:
    from pathlib import Path
except ImportError:  # pragma: no cover
    from pathlib2 import Path


class Repo(object):
    """Represent a Renga repository."""

    METADATA = 'metadata.yml'
    """Default name of Renga config file."""

    LOCK_SUFFIX = '.lock'
    """Default suffix for Renga lock file."""

    def __init__(self, renga=None):
        """Store core options."""
        self.renga_path = renga or RENGA_HOME

    @property
    def path(self):
        """Return a ``Path`` instance of this repository."""
        return Path(get_git_home())

    @property
    def git(self):
        """Return a Git repository."""
        return GitRepo(str(self.path))

    @property
    def renga_path(self):
        """Return a ``Path`` instance of Renga folder."""
        path = Path(self._renga_path)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        return path

    @renga_path.setter
    def renga_path(self, value):
        """Update path of the Renga folder."""
        self._renga_path = value

    @property
    def lock(self):
        """Create a Renga config lock."""
        return filelock.FileLock(
            str(self.renga_path.with_suffix(self.LOCK_SUFFIX)))

    @property
    def renga_metadata_path(self):
        """Return a ``Path`` instance of Renga metadata file."""
        return self.renga_path.joinpath(self.METADATA)

    @contextmanager
    def with_metadata(self):
        """Yield a editable metadata object."""
        with self.lock:
            path = str(self.renga_metadata_path)
            metadata = read_config(path, final=True)
            yield metadata
            write_config(metadata, path, final=True)

    def init(self, name=None, force=False):
        """Initialize a Renga repository."""
        self.renga_path.mkdir(parents=True, exist_ok=force)

        path = self.path.absolute()
        if force:
            try:
                git = GitRepo.init(str(path))
            except FileExistsError:
                git = GitRepo(str(path))
        else:
            git = GitRepo.init(str(path))

        git.set_description((name or path.name).encode('utf-8'))

        with self.with_metadata() as metadata:
            metadata.setdefault('version', 1)
            assert metadata['version'] == 1, 'Only version 1 is supported.'
            metadata.setdefault('core', {})
            metadata['core']['name'] = name
            metadata['core'].setdefault(
                'created', datetime.datetime.now().isoformat())

        return str(path)

    @property
    def state(self):
        """Return the current state object."""
        raise NotImplemented()  # pragma: no cover


pass_repo = click.make_pass_decorator(Repo, ensure=True)
