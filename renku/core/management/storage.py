# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Client for handling a data storage."""
import functools
import os
import shlex
from collections import defaultdict
from pathlib import Path
from shutil import which
from subprocess import PIPE, STDOUT, call, run

import attr
from werkzeug.utils import cached_property

from renku.core import errors

from .git import _expand_directories
from .repository import RepositoryApiMixin

# Batch size for when renku is expanding a large list
# of files into an argument string.
ARGUMENT_BATCH_SIZE = 100


def ensure_external_storage(fn):
    """Ensure management of external storage on methods which depend on it.

    :raises: ``errors.ExternalStorageNotInstalled``
    :raises: ``errors.ExternalStorageDisabled``
    """
    # noqa
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self.has_external_storage:
            pass
        else:
            return fn(self, *args, **kwargs)

    return wrapper


@attr.s
class StorageApiMixin(RepositoryApiMixin):
    """Client for handling a data storage."""

    use_external_storage = attr.ib(default=True)
    """Use external storage (e.g. LFS)."""

    _CMD_STORAGE_INSTALL = ['git', 'lfs', 'install', '--local']

    _CMD_STORAGE_TRACK = ['git', 'lfs', 'track', '--']

    _CMD_STORAGE_UNTRACK = ['git', 'lfs', 'untrack', '--']

    _CMD_STORAGE_CHECKOUT = ['git', 'lfs', 'checkout']

    _CMD_STORAGE_PULL = ['git', 'lfs', 'pull', '-I']

    @cached_property
    def storage_installed(self):
        """Verify that git-lfs is installed and on system PATH."""
        return bool(which('git-lfs'))

    @cached_property
    def has_external_storage(self):
        """Check if repository has external storage enabled.

        :raises: ``errors.ExternalStorageNotInstalled``
        :raises: ``errors.ExternalStorageDisabled``
        """
        repo_config = self.repo.config_reader(config_level='repository')
        lfs_enabled = repo_config.has_section('filter "lfs"')

        storage_enabled = lfs_enabled and self.storage_installed
        if self.use_external_storage and not storage_enabled:
            raise errors.ExternalStorageDisabled(self.repo)

        if lfs_enabled and not self.storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        return lfs_enabled and self.storage_installed

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        try:
            call(
                self._CMD_STORAGE_INSTALL + (['--force'] if force else []),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=str(self.path.absolute()),
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )

    def init_repository(self, force=False):
        """Initialize a local Renku repository."""
        result = super().init_repository(force=force)

        # initialize LFS if it is requested and installed
        if self.use_external_storage and self.storage_installed:
            self.init_external_storage(force=force)

        return result

    @ensure_external_storage
    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        # Calculate which paths can be tracked in lfs
        if not self.use_external_storage:
            return

        track_paths = []
        attrs = self.find_attr(*paths)

        for path in paths:
            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(path, {}).get('filter') == 'lfs':
                continue

            path = Path(path)

            # Do not track symlinks in LFS
            if path.is_symlink():
                continue

            if path.is_dir():
                track_paths.append(str(path / '**'))
            elif path.suffix != '.ipynb':
                # TODO create configurable filter and follow .gitattributes
                track_paths.append(str(path))

        if track_paths:
            try:
                call(
                    self._CMD_STORAGE_TRACK + track_paths,
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=str(self.path),
                )
            except (KeyboardInterrupt, OSError) as e:
                raise errors.ParameterError(
                    'Couldn\'t run \'git lfs\':\n{0}'.format(e)
                )

    @ensure_external_storage
    def untrack_paths_from_storage(self, *paths):
        """Untrack paths from the external storage."""
        try:
            call(
                self._CMD_STORAGE_UNTRACK + list(paths),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=str(self.path),
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )

    @ensure_external_storage
    def pull_paths_from_storage(self, *paths):
        """Pull paths from LFS."""
        import math
        client_dict = defaultdict(list)

        for path in _expand_directories(paths):
            client, commit, path = self.resolve_in_submodules(
                self.repo.commit(), path
            )
            try:
                absolute_path = Path(path).resolve()
                relative_path = absolute_path.relative_to(client.path)
            except ValueError:  # An external file
                absolute_path = Path(os.path.abspath(path))
                relative_path = absolute_path.relative_to(client.path)
            client_dict[client.path].append(str(relative_path))

        for client_path, paths in client_dict.items():
            batch_size = math.ceil(len(paths) / ARGUMENT_BATCH_SIZE)
            for index in range(batch_size):
                run(
                    self._CMD_STORAGE_PULL + [
                        shlex.quote(
                            ','.join(
                                paths[index * ARGUMENT_BATCH_SIZE:(index + 1) *
                                      ARGUMENT_BATCH_SIZE]
                            )
                        )
                    ],
                    cwd=str(client_path.absolute()),
                    stdout=PIPE,
                    stderr=STDOUT,
                )

    @ensure_external_storage
    def checkout_paths_from_storage(self, *paths):
        """Checkout a paths from LFS."""
        run(
            self._CMD_STORAGE_CHECKOUT + list(paths),
            cwd=str(self.path.absolute()),
            stdout=PIPE,
            stderr=STDOUT,
            check=True,
        )
