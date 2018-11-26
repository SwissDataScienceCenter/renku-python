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
"""Client for handling a data storage."""

from subprocess import PIPE, STDOUT, call, run

import attr

from renku import errors
from renku._compat import Path

from .repository import RepositoryApiMixin

HAS_LFS = call(['git', 'lfs'], stdout=PIPE, stderr=STDOUT) == 0


@attr.s
class StorageApiMixin(RepositoryApiMixin):
    """Client for handling a data storage."""

    use_external_storage = attr.ib(default=True)
    """Use external storage (e.g. LFS)."""

    _CMD_STORAGE_INSTALL = ['git', 'lfs', 'install', '--local']

    _CMD_STORAGE_TRACK = ['git', 'lfs', 'track']

    _CMD_STORAGE_PULL = ['git', 'lfs', 'pull', 'origin', '-I']

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
        return HAS_LFS and self.repo.config_reader(
        ).has_section('filter "lfs"')

    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if self.use_external_storage and self.external_storage_installed:
            track_paths = []
            for path in paths:
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
        elif self.use_external_storage:
            raise errors.ExternalStorageNotInstalled(self.repo)

    def pull_path(self, path):
        """Pull a path from LFS."""
        client, commit, path = self.resolve_in_submodules(
            self.repo.commit(), path
        )
        run(
            self._CMD_STORAGE_PULL + [str(path)],
            cwd=str(client.path.absolute()),
            check=True
        )

    def init_repository(self, name=None, force=False):
        """Initialize a local Renku repository."""
        result = super().init_repository(name=name, force=force)

        # initialize LFS if it is requested and installed
        if self.use_external_storage and HAS_LFS:
            self.init_external_storage(force=force)

        return result
