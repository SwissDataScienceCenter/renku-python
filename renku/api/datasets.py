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
"""Client for handling datasets."""

import os
import shutil
import stat
import warnings
from configparser import NoSectionError
from contextlib import contextmanager
from urllib import error, parse

import attr
import requests
import yaml

from renku import errors
from renku._compat import Path
from renku.models._git import GitURL
from renku.models.datasets import Author, Dataset, DatasetFile, NoneType


@attr.s
class DatasetsApiMixin(object):
    """Client for handling datasets."""

    datadir = attr.ib(default='data', converter=str)
    """Define a name of the folder for storing datasets."""

    DATASETS = 'datasets'
    """Directory for storing dataset metadata in Renku."""

    @property
    def renku_datasets_path(self):
        """Return a ``Path`` instance of Renku dataset metadata folder."""
        return self.renku_path.joinpath(self.DATASETS)

    def datasets_from_commit(self, commit=None):
        """Return datasets defined in a commit."""
        commit = commit or self.repo.head.commit

        try:
            datasets = commit.tree / self.renku_home / self.DATASETS
        except KeyError:
            return

        for tree in datasets:
            try:
                blob = tree / self.METADATA
            except KeyError:
                continue

            yield Dataset.from_jsonld(
                yaml.safe_load(blob.data_stream.read()),
                __reference__=Path(blob.path),
            )

    @property
    def datasets(self):
        """Return mapping from path to dataset."""
        result = {}
        for path in self.renku_datasets_path.rglob(self.METADATA):
            result[path] = Dataset.from_yaml(path)
        return result

    def load_dataset(self, name=None):
        """Load dataset reference file."""
        from renku.models.refs import LinkReference
        path = None
        dataset = None

        if name:
            path = self.renku_datasets_path / name / self.METADATA

            if not path.exists():
                path = LinkReference(
                    client=self, name='datasets/' + name
                ).reference

            if path.exists():
                dataset = Dataset.from_yaml(path)

        return dataset

    def store_dataset(self, dataset):
        """Store dataset reference file."""
        with dataset.__reference__.open('w') as f:
            yaml.dump(dataset.asjsonld(), f, default_flow_style=False)

    @contextmanager
    def with_dataset(self, name=None):
        """Yield an editable metadata object for a dataset."""
        from renku.models.refs import LinkReference
        with self.lock:
            dataset = self.load_dataset(name=name)

            if dataset is None:
                dataset = Dataset(name=name)
                setattr(dataset, '__source__', {})

                path = (
                    self.renku_datasets_path / dataset.identifier.hex /
                    self.METADATA
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                setattr(dataset, '__reference__', path)

                if name:
                    LinkReference.create(
                        client=self, name='datasets/' + name
                    ).set_reference(path)

            dataset_path = self.path / self.datadir / dataset.name
            dataset_path.mkdir(parents=True, exist_ok=True)

            yield dataset

            # TODO
            # if path is None:
            #     path = dataset_path / self.METADATA
            #     if path.exists():
            #         raise ValueError('Dataset already exists')

            self.store_dataset(dataset)

    def add_data_to_dataset(
        self, dataset, url, git=False, force=False, **kwargs
    ):
        """Import the data into the data directory."""
        dataset_path = self.path / self.datadir / dataset.name
        git = git or check_for_git_repo(url)

        target = kwargs.pop('target', None)

        if git:
            if isinstance(target, (str, NoneType)):
                files = self._add_from_git(
                    dataset, dataset_path, url, target, **kwargs
                )
            else:
                files = {}
                for t in target:
                    files.update(
                        self._add_from_git(
                            dataset, dataset_path, url, t, **kwargs
                        )
                    )
        else:
            files = self._add_from_url(dataset, dataset_path, url, **kwargs)

        ignored = self.find_ignored_paths(
            *[
                os.path.relpath(
                    str(
                        self.renku_datasets_path / dataset.identifier.hex / key
                    ),
                    start=str(self.path),
                ) for key in files.keys()
            ]
        )

        if ignored:
            if force:
                self.repo.git.add(*ignored, force=True)
            else:
                raise errors.IgnoredFiles(ignored)

        dataset.files.update(files)

    def _add_from_url(self, dataset, path, url, link=False, **kwargs):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme)
            )

        # Respect the directory struture inside the source path.
        relative_to = kwargs.pop('relative_to', None)
        if relative_to:
            dst_path = Path(url).resolve().absolute().relative_to(
                Path(relative_to).resolve().absolute()
            )
        else:
            dst_path = os.path.basename(url)

        dst = path.joinpath(dst_path).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            # if we have a directory, recurse
            if src.is_dir():
                files = {}
                dst.mkdir(parents=True, exist_ok=True)
                for f in src.iterdir():
                    files.update(
                        self._add_from_url(
                            dataset,
                            dst,
                            f.absolute().as_posix(),
                            link=link,
                        )
                    )
                return files

            # Make sure the parent directory exists.
            dst.parent.mkdir(parents=True, exist_ok=True)

            if link:
                try:
                    os.link(str(src), str(dst))
                except Exception as e:
                    raise Exception(
                        'Could not create hard link '
                        '- retry without --link.'
                    ) from e
            else:
                shutil.copy(str(src), str(dst))

            # Do not expose local paths.
            src = None
        else:
            try:
                response = requests.get(url)
                dst.write_bytes(response.content)
            except error.HTTPError as e:  # pragma nocover
                raise e

        # make the added file read-only
        mode = dst.stat().st_mode & 0o777
        dst.chmod(mode & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))

        self.track_paths_in_storage(str(dst.relative_to(self.path)))
        dataset_path = self.renku_datasets_path / dataset.name
        result = os.path.relpath(str(dst), start=str(dataset_path))
        return {
            result:
                DatasetFile(
                    path=result,
                    url=url,
                    authors=dataset.authors,
                    dataset=dataset.name,
                )
        }

    def _add_from_git(self, dataset, path, url, target, **kwargs):
        """Process adding resources from another git repository.

        The submodules are placed in ``.renku/vendors`` and linked
        to the *path* specified by the user.
        """
        from git import Repo

        # create the submodule
        if url.startswith('git@'):
            url = 'git+ssh://' + url

        u = parse.urlparse(url)
        submodule_path = self.renku_path / 'vendors' / (u.netloc or 'local')

        # Respect the directory struture inside the source path.
        relative_to = kwargs.get('relative_to', None)

        if u.scheme in ('', 'file'):
            try:
                relative_url = Path(url).resolve().relative_to(self.path)
            except Exception:
                relative_url = None

            if relative_url:
                result = str(
                    os.path.relpath(
                        str(relative_url),
                        start=str(
                            self.renku_datasets_path / dataset.identifier.hex
                        ),
                    )
                )
                return {
                    result:
                        DatasetFile(
                            path=result,
                            url=url,
                            authors=dataset.authors,
                            dataset=dataset.name,
                        )
                }

            warnings.warn('Importing local git repository, use HTTPS')
            # determine where is the base repo path
            r = Repo(url, search_parent_directories=True)
            src_repo_path = Path(r.git_dir).parent.resolve()
            submodule_name = src_repo_path.name
            submodule_path = submodule_path / str(src_repo_path).lstrip('/')

            # if repo path is a parent, rebase the paths and update url
            if src_repo_path != Path(u.path):
                top_target = Path(
                    u.path
                ).resolve().absolute().relative_to(src_repo_path)
                if target:
                    target = top_target / target
                else:
                    target = top_target
                url = src_repo_path.as_posix()
        elif u.scheme in {'http', 'https', 'git+https', 'git+ssh'}:
            submodule_name = os.path.splitext(os.path.basename(u.path))[0]
            submodule_path = submodule_path.joinpath(
                os.path.dirname(u.path).lstrip('/'), submodule_name
            )
        else:
            raise NotImplementedError(
                'Scheme {} not supported'.format(u.scheme)
            )

        # FIXME: do a proper check that the repos are not the same
        if submodule_name not in (s.name for s in self.repo.submodules):
            if u.scheme in {'http', 'https', 'git+https', 'git+ssh'}:
                url = self.get_relative_url(url)

            # Submodule in python git does some custom magic that does not
            # allow for relative URLs, so we call the git function directly
            self.repo.git.submodule([
                'add', '--force', '--name', submodule_name, url,
                submodule_path.relative_to(self.path).as_posix()
            ])

        src = submodule_path / (target or '')

        if target and relative_to:
            relative_to = Path(relative_to)
            if relative_to.is_absolute():
                assert u.scheme in {
                    '', 'file'
                }, ('Only relative paths can be used with URLs.')
                target = (Path(url).resolve().absolute() / target).relative_to(
                    relative_to.resolve()
                )
            else:
                # src already includes target so we do not have to append it
                target = src.relative_to(submodule_path / relative_to)

        # link the target into the data directory
        dst = self.path / path / (target or '')

        # if we have a directory, recurse
        if src.is_dir():
            files = {}
            dst.mkdir(parents=True, exist_ok=True)
            # FIXME get all files from submodule index
            for f in src.iterdir():
                try:
                    files.update(
                        self._add_from_git(
                            dataset,
                            path,
                            url,
                            target=f.relative_to(submodule_path),
                            **kwargs
                        )
                    )
                except ValueError:
                    pass  # skip files outside the relative path
            return files

        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        os.symlink(os.path.relpath(str(src), str(dst.parent)), str(dst))

        # grab all the authors from the commit history
        git_repo = Repo(str(submodule_path.absolute()))
        authors = []
        for commit in git_repo.iter_commits(paths=target):
            author = Author.from_commit(commit)
            if author not in authors:
                authors.append(author)

        dataset_path = self.renku_datasets_path / dataset.name
        result = os.path.relpath(str(dst), start=str(dataset_path))

        if u.scheme in ('', 'file'):
            url = None
        else:
            url = '{}/{}'.format(url, target)

        return {
            result:
                DatasetFile(
                    path=result,
                    url=url,
                    authors=authors,
                    dataset=dataset.name,  # TODO detect original dataset
                )
        }

    def get_relative_url(self, url):
        """Determine if the repo url should be relative."""
        # Check if the default remote of the branch we are on is on
        # the same server as the submodule. If so, use a relative path
        # instead of an absolute URL.
        try:
            branch_remote = self.repo.config_reader().get(
                'branch "{}"'.format(self.repo.active_branch.name), 'remote'
            )
        except NoSectionError:
            branch_remote = 'origin'

        try:
            remote = self.repo.remote(branch_remote)
        except ValueError:
            warnings.warn(
                'Remote {} not found, cannot check for relative URL.'.
                format(branch_remote)
            )
            return url

        remote_url = GitURL.parse(remote.url)
        submodule_url = GitURL.parse(url)

        if remote_url.hostname == submodule_url.hostname:
            # construct the relative path
            url = Path(
                '../../{}'.format(submodule_url.owner) if remote_url.owner ==
                submodule_url.owner else '..'
            )
            url = str(url / submodule_url.name)
        return url


def check_for_git_repo(url):
    """Check if a url points to a git repository."""
    u = parse.urlparse(url)
    is_git = False

    if os.path.splitext(u.path)[1] == '.git':
        is_git = True
    elif u.scheme in ('', 'file'):
        from git import InvalidGitRepositoryError, Repo

        try:
            Repo(u.path, search_parent_directories=True)
            is_git = True
        except InvalidGitRepositoryError:
            is_git = False
    return is_git
