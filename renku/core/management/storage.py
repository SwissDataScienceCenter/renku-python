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
import re
import shlex
import tempfile
from collections import defaultdict
from pathlib import Path
from shutil import move, which
from subprocess import PIPE, STDOUT, call, check_output, run

import attr
import pathspec
from werkzeug.utils import cached_property

from renku.core import errors
from renku.core.utils.file_size import parse_file_size

from .git import _expand_directories
from .repository import RepositoryApiMixin

# Batch size for when renku is expanding a large list
# of files into an argument string.
ARGUMENT_BATCH_SIZE = 100


def check_external_storage_wrapper(fn):
    """Check availability of external storage on methods that need it.

    :raises: ``errors.ExternalStorageNotInstalled``
    :raises: ``errors.ExternalStorageDisabled``
    """
    # noqa
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self.check_external_storage():
            pass
        else:
            return fn(self, *args, **kwargs)

    return wrapper


@attr.s
class StorageApiMixin(RepositoryApiMixin):
    """Client for handling a data storage."""

    external_storage_requested = attr.ib(default=True)
    """External storage (e.g. LFS) requested for Renku command."""

    RENKU_LFS_IGNORE_PATH = '.renkulfsignore'
    """.gitignore like file specifying paths that are not tracked in LFS."""

    _CMD_STORAGE_INSTALL = ['git', 'lfs', 'install', '--local']

    _CMD_STORAGE_TRACK = ['git', 'lfs', 'track', '--']

    _CMD_STORAGE_UNTRACK = ['git', 'lfs', 'untrack', '--']

    _CMD_STORAGE_CLEAN = ['git', 'lfs', 'clean']

    _CMD_STORAGE_CHECKOUT = ['git', 'lfs', 'checkout']

    _CMD_STORAGE_PULL = ['git', 'lfs', 'pull', '-I']

    _CMD_STORAGE_MIGRATE_INFO = [
        'git', 'lfs', 'migrate', 'info', '--top', '42000'
    ]

    _CMD_STORAGE_LIST = ['git', 'lfs', 'ls-files', '-n']

    _CMD_STORAGE_STATUS = ['git', 'lfs', 'status']

    _LFS_HEADER = 'version https://git-lfs.github.com/spec/'

    @cached_property
    def storage_installed(self):
        """Verify that git-lfs is installed and on system PATH."""
        return bool(which('git-lfs'))

    @cached_property
    def storage_installed_locally(self):
        """Verify that git-lfs is installed for the project."""
        repo_config = self.repo.config_reader(config_level='repository')
        return repo_config.has_section('filter "lfs"')

    def check_external_storage(self):
        """Check if repository has external storage enabled.

        :raises: ``errors.ExternalStorageNotInstalled``
        :raises: ``errors.ExternalStorageDisabled``
        """
        storage_installed = (
            self.storage_installed_locally and self.storage_installed
        )
        if self.external_storage_requested and not storage_installed:
            raise errors.ExternalStorageDisabled(self.repo)

        if self.storage_installed_locally and not self.storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repo)

        return storage_installed

    @cached_property
    def renku_lfs_ignore(self):
        """Gets pathspec for files to not add to LFS."""
        ignore_path = self.path / self.RENKU_LFS_IGNORE_PATH
        if not os.path.exists(ignore_path):
            return pathspec.PathSpec.from_lines('renku_gitwildmatch', [])
        with ignore_path.open('r') as f:
            return pathspec.PathSpec.from_lines('renku_gitwildmatch', f)

    @property
    def minimum_lfs_file_size(self):
        """The minimum size of a file in bytes to be added to lfs."""
        size = self.get_value('renku', 'lfs_threshold') or '100kb'

        return parse_file_size(size)

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        try:
            call(
                self._CMD_STORAGE_INSTALL + (['--force'] if force else []),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=self.path,
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )

    def init_repository(self, force=False):
        """Initialize a local Renku repository."""
        result = super().init_repository(force=force)

        # initialize LFS if it is requested and installed
        if self.external_storage_requested and self.storage_installed:
            self.init_external_storage(force=force)

        return result

    @check_external_storage_wrapper
    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if not self.external_storage_requested:
            return

        # Calculate which paths can be tracked in lfs
        track_paths = []
        attrs = self.find_attr(*paths)

        for path in paths:
            path = Path(path)

            # Do not track symlinks in LFS
            if path.is_symlink():
                continue

            if path.is_absolute():
                path = Path(path).relative_to(self.path)

            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(str(path), {}).get('filter') == 'lfs':
                continue

            if (
                path.is_dir() and
                not any(self.renku_lfs_ignore.match_tree(str(path)))
            ):
                track_paths.append(str(path / '**'))
            elif not self.renku_lfs_ignore.match_file(str(path)):
                file_size = os.path.getsize(
                    str(os.path.relpath(self.path / path, os.getcwd()))
                )
                if file_size >= self.minimum_lfs_file_size:
                    track_paths.append(str(path))

        if track_paths:
            try:
                call(
                    self._CMD_STORAGE_TRACK + track_paths,
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=self.path,
                )
            except (KeyboardInterrupt, OSError) as e:
                raise errors.ParameterError(
                    'Couldn\'t run \'git lfs\':\n{0}'.format(e)
                )
            return track_paths
        return []

    @check_external_storage_wrapper
    def untrack_paths_from_storage(self, *paths):
        """Untrack paths from the external storage."""
        try:
            call(
                self._CMD_STORAGE_UNTRACK + list(paths),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=self.path,
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )

    @check_external_storage_wrapper
    def list_tracked_paths(self, client=None):
        """List paths tracked in lfs for a client."""
        client = client or self
        try:
            files = check_output(
                self._CMD_STORAGE_LIST, cwd=client.path, encoding='UTF-8'
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )
        files = [client.path / f for f in files.splitlines()]
        return files

    @check_external_storage_wrapper
    def list_unpushed_lfs_paths(self, client=None):
        """List paths tracked in lfs for a client."""
        client = client or self

        if (
            len(client.repo.remotes) < 1 or
            not client.repo.active_branch.tracking_branch()
        ):
            raise errors.ConfigurationError(
                'No git remote is configured for {} branch {}.'.
                format(client.path, client.repo.active_branch.name) +
                'Cleaning the storage cache would lead to a loss of data as ' +
                'it is not on a server. Please see ' +
                'https://www.atlassian.com/git/tutorials/syncing for ' +
                'information on how to sync with a remote.'
            )
        try:
            status = check_output(
                self._CMD_STORAGE_STATUS, cwd=client.path, encoding='UTF-8'
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(
                'Couldn\'t run \'git lfs\':\n{0}'.format(e)
            )

        files = status.split('Objects to be committed:')[0].splitlines()[2:]
        files = [
            client.path / f.rsplit('(', 1)[0].strip()
            for f in files if f.strip()
        ]
        return files

    @check_external_storage_wrapper
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
                    cwd=client_path,
                    stdout=PIPE,
                    stderr=STDOUT,
                )

    @check_external_storage_wrapper
    def clean_storage_cache(self, *paths):
        """Remove paths from lfs cache."""
        client_dict = defaultdict(list)
        clients = {}
        tracked_paths = defaultdict(list)
        unpushed_paths = defaultdict(list)
        untracked_paths = []
        local_only_paths = []

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

            if client.path not in tracked_paths:
                tracked_paths[client.path] = self.list_tracked_paths(client)

            if client.path not in unpushed_paths:
                u_paths = self.list_unpushed_lfs_paths(client)
                unpushed_paths[client.path] = u_paths

            if absolute_path in unpushed_paths[client.path]:
                local_only_paths.append(str(relative_path))
            elif absolute_path not in tracked_paths[client.path]:
                untracked_paths.append(str(relative_path))
            else:
                client_dict[client.path].append(str(relative_path))
                clients[client.path] = client

        for client_path, paths in client_dict.items():
            client = clients[client_path]

            for path in paths:
                with open(path, 'r') as tracked_file:
                    try:
                        header = tracked_file.read(len(self._LFS_HEADER))
                        if header == self._LFS_HEADER:
                            # file is not pulled
                            continue
                    except UnicodeDecodeError:
                        # likely a binary file, not lfs pointer file
                        pass
                with tempfile.NamedTemporaryFile(
                    mode='w+t', encoding='utf-8', delete=False
                ) as tmp, open(path, 'r+t') as input_file:
                    run(
                        self._CMD_STORAGE_CLEAN,
                        cwd=client_path,
                        stdin=input_file,
                        stdout=tmp,
                    )

                    tmp_path = tmp.name
                move(tmp_path, path)

                # get lfs sha hash
                old_pointer = client.repo.git.show('HEAD:{}'.format(path))
                old_pointer = old_pointer.splitlines()[1]
                old_pointer = old_pointer.split(' ')[1].split(':')[1]

                prefix1 = old_pointer[:2]
                prefix2 = old_pointer[2:4]

                # remove from lfs cache
                object_path = (
                    client.path / '.git' / 'lfs' / 'objects' / prefix1 /
                    prefix2 / old_pointer
                )
                object_path.unlink()

            # add paths so they don't show as modified
            client.repo.git.add(*paths)

        return untracked_paths, local_only_paths

    @check_external_storage_wrapper
    def checkout_paths_from_storage(self, *paths):
        """Checkout a paths from LFS."""
        run(
            self._CMD_STORAGE_CHECKOUT + list(paths),
            cwd=self.path,
            stdout=PIPE,
            stderr=STDOUT,
            check=True,
        )

    def check_requires_tracking(self, *paths):
        """Check paths and return a list of those that must be tracked."""
        if not self.external_storage_requested:
            return

        attrs = self.find_attr(*paths)
        track_paths = []

        for path in paths:
            absolute_path = Path(os.path.abspath(self.path / path))
            path = str(path)

            # Do not track symlinks in LFS
            if absolute_path.is_symlink():
                continue

            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(path, {}).get('filter') == 'lfs':
                continue

            if not absolute_path.is_dir():
                if self.renku_lfs_ignore.match_file(path):
                    continue
                if os.path.getsize(absolute_path) < self.minimum_lfs_file_size:
                    continue

                track_paths.append(path)

        return track_paths

    def check_lfs_migrate_info(self, everything=False):
        """Return list of file groups in history should be in LFS."""
        ref = ['--everything'] if everything else [
            '--include-ref', self.repo.active_branch.name
        ]

        includes = []
        excludes = []
        for p in self.renku_lfs_ignore.patterns:
            if p.regex is None:
                continue

            pattern = p.pattern.replace(os.linesep, '').replace('\n', '')
            if pattern.startswith('!'):
                pattern.replace('!', '', 1)

            if p.include:  # File ignored by LFS
                excludes.append(pattern)
            else:
                includes.append(pattern)

        if excludes:
            excludes = ['--exclude', ','.join(excludes)]
        if includes:
            includes = ['--include', ','.join(includes)]

        above = ['--above', str(self.minimum_lfs_file_size)]

        command = (
            self._CMD_STORAGE_MIGRATE_INFO + ref + above + includes + excludes
        )

        try:
            lfs_output = run(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=self.path,
                universal_newlines=True
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.GitError(
                'Couldn\'t run \'git lfs migrate info\':\n{0}'.format(e)
            )

        groups = []
        files_re = re.compile(r'(.*\s+[\d.]+\s+\S+).*')

        for line in lfs_output.stdout.split('\n'):
            match = files_re.match(line)
            if match:
                groups.append(match.groups()[0])

        return groups
