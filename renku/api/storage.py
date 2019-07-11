# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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

import shlex
from collections import defaultdict
from subprocess import PIPE, STDOUT, call, run

import attr

from renku import errors
from renku._compat import Path

from ._git import _expand_directories
from .repository import RepositoryApiMixin

HAS_LFS = call(['git', 'lfs'], stdout=PIPE, stderr=STDOUT) == 0

# Batch size for when renku is expanding a large list
# of files into an argument string.
ARGUMENT_BATCH_SIZE = 100


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

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        call(
            self._CMD_STORAGE_INSTALL + (['--force'] if force else []),
            stdout=PIPE,
            stderr=STDOUT,
            cwd=str(self.path.absolute()),
        )

    @property
    def external_storage_installed(self):
        """Check that Large File Storage is installed."""
        return HAS_LFS

    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if not self._use_lfs():
            return

        if not self.external_storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        track_paths = []
        attrs = self.find_attr(*paths)

        for path in paths:
            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(path, {}).get('filter') == 'lfs':
                continue

            path = Path(path)
            if path.is_dir():
                track_paths.append(str(path / '**'))
            elif path.suffix != '.ipynb':
                # TODO create configurable filter and follow .gitattributes
                track_paths.append(str(path))

        call(
            self._CMD_STORAGE_TRACK + track_paths,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=str(self.path),
        )

    def untrack_paths_from_storage(self, *paths):
        """Untrack paths from the external storage."""
        if not self._use_lfs():
            return

        if not self.external_storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        call(
            self._CMD_STORAGE_UNTRACK + list(paths),
            stdout=PIPE,
            stderr=STDOUT,
            cwd=str(self.path),
        )

    def pull_paths_from_storage(self, *paths):
        """Pull paths from LFS."""
        import math

        if not self._use_lfs():
            return

        if not self.external_storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        client_dict = defaultdict(list)

        for path in _expand_directories(paths):
            client, commit, path = self.resolve_in_submodules(
                self.repo.commit(), path
            )
            client_dict[client.path].append(str(path))

        for client_path, paths in client_dict.items():
            for ibatch in range(math.ceil(len(paths) / ARGUMENT_BATCH_SIZE)):
                run(
                    self._CMD_STORAGE_PULL + [
                        shlex.quote(
                            ','.join(
                                paths[ibatch * ARGUMENT_BATCH_SIZE:
                                      (ibatch + 1) * ARGUMENT_BATCH_SIZE]
                            )
                        )
                    ],
                    cwd=str(client_path.absolute()),
                    stdout=PIPE,
                    stderr=STDOUT,
                )

    def checkout_paths_from_storage(self, *paths):
        """Checkout a paths from LFS."""
        if not self._use_lfs():
            return

        if not self.external_storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        run(
            self._CMD_STORAGE_CHECKOUT + list(paths),
            cwd=str(self.path.absolute()),
            stdout=PIPE,
            stderr=STDOUT,
            check=True,
        )

    def init_repository(self, name=None, force=False):
        """Initialize a local Renku repository."""
        result = super().init_repository(name=name, force=force)

        # initialize LFS if it is requested and installed
        if self.use_external_storage and self.external_storage_installed:
            self.init_external_storage(force=force)

        return result

    def _use_lfs(self):
        renku_initialized_to_use_lfs = self.repo.config_reader(
            config_level='repository'
        ).has_section('filter "lfs"')

        return renku_initialized_to_use_lfs and self.use_external_storage
