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
import re
import shutil
import stat
import uuid
import warnings
from configparser import NoSectionError
from contextlib import contextmanager
from pathlib import Path
from urllib import error, parse

import attr
import requests

from renku.core import errors
from renku.core.management.config import RENKU_HOME
from renku.core.models.creators import Creator
from renku.core.models.datasets import Dataset, DatasetFile, DatasetTag, \
    NoneType
from renku.core.models.git import GitURL
from renku.core.models.locals import with_reference
from renku.core.models.refs import LinkReference


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
        return Path(self.renku_home).joinpath(self.DATASETS)

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
            dataset = Dataset.from_yaml(
                self.path / Path(blob.path), client=self
            )
            dataset.commit = commit
            yield dataset

    @property
    def datasets(self):
        """Return mapping from path to dataset."""
        result = {}
        for path in self.renku_datasets_path.rglob(self.METADATA):
            result[path] = self.get_dataset(path)
        return result

    def get_dataset(self, path, commit=None):
        """Return a dataset from a given path."""
        if not path.is_absolute():
            path = self.path / path
        return Dataset.from_yaml(path, client=self, commit=commit)

    def dataset_path(self, name):
        """Get dataset path from name."""
        path = self.renku_datasets_path / name / self.METADATA
        if not path.exists():
            path = LinkReference(
                client=self, name='datasets/' + name
            ).reference

        return path

    def load_dataset(self, name=None):
        """Load dataset reference file."""
        if name:
            path = self.dataset_path(name)
            if path.exists():
                return self.get_dataset(path)

    @contextmanager
    def with_dataset(self, name=None):
        """Yield an editable metadata object for a dataset."""
        dataset = self.load_dataset(name=name)

        if dataset is None:
            identifier = str(uuid.uuid4())
            path = (self.renku_datasets_path / identifier / self.METADATA)
            path.parent.mkdir(parents=True, exist_ok=True)

            with with_reference(path):
                dataset = Dataset(
                    identifier=identifier, name=name, client=self
                )

            if name:
                LinkReference.create(client=self, name='datasets/' +
                                     name).set_reference(path)

        dataset_path = self.path / self.datadir / dataset.name
        dataset_path.mkdir(parents=True, exist_ok=True)

        yield dataset

        # TODO
        # if path is None:
        #     path = dataset_path / self.METADATA
        #     if path.exists():
        #         raise ValueError('Dataset already exists')

        dataset.to_yaml()

    def add_data_to_dataset(
        self, dataset, urls, git=False, force=False, **kwargs
    ):
        """Import the data into the data directory."""
        dataset_path = self.path / self.datadir / dataset.name

        files = []

        for url in urls:
            git = git or check_for_git_repo(url)

            target = kwargs.pop('target', None)

            if git:
                if isinstance(target, (str, NoneType)):
                    files.extend(
                        self._add_from_git(
                            dataset, dataset_path, url, target, **kwargs
                        )
                    )
                else:
                    for t in target:
                        files.extend(
                            self._add_from_git(
                                dataset, dataset_path, url, t, **kwargs
                            )
                        )
            else:
                files.extend(
                    self._add_from_url(dataset, dataset_path, url, **kwargs)
                )

        ignored = self.find_ignored_paths(*(data['path']
                                            for data in files)) or []

        if ignored:
            if force:
                self.repo.git.add(*ignored, force=True)
            else:
                raise errors.IgnoredFiles(ignored)

        # commit all new data
        file_paths = {str(data['path']) for data in files if str(data['path'])}
        self.repo.git.add(*(file_paths - set(ignored)))
        self.repo.index.commit(
            'renku dataset: commiting {} newly added files'.
            format(len(file_paths) + len(ignored))
        )

        # Generate the DatasetFiles
        dataset_files = []
        for data in files:
            datasetfile = DatasetFile.from_revision(self, **data)

            # Set dataset file path relative to projects root for submodules
            if datasetfile.client != self:
                datasetfile.path = str(data['path'])
            dataset_files.append(datasetfile)
        dataset.update_files(dataset_files)

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
            dst_path = Path(u.path).resolve().absolute().relative_to(
                Path(relative_to).resolve().absolute()
            )
        else:
            dst_path = os.path.basename(u.path)

        dst = path.joinpath(dst_path).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            # if we have a directory, recurse
            if src.is_dir():
                files = []
                dst.mkdir(parents=True, exist_ok=True)
                for f in src.iterdir():
                    files.extend(
                        self._add_from_url(
                            dataset,
                            dst,
                            f.absolute().as_posix(),
                            link=link,
                            **kwargs
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

        return [{
            'path': dst.relative_to(self.path),
            'url': url,
            'creator': dataset.creator,
            'dataset': dataset.name,
            'parent': self
        }]

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
                return [{
                    'path': url,
                    'url': url,
                    'creator': dataset.creator,
                    'dataset': dataset.name,
                    'parent': self
                }]

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
                }, 'Only relative paths can be used with URLs.'
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
            files = []
            dst.mkdir(parents=True, exist_ok=True)
            # FIXME get all files from submodule index
            for f in src.iterdir():
                try:
                    files.extend(
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

        # grab all the creators from the commit history
        git_repo = Repo(str(submodule_path.absolute()))
        creators = []
        for commit in git_repo.iter_commits(paths=target):
            creator = Creator.from_commit(commit)
            if creator not in creators:
                creators.append(creator)

        if u.scheme in ('', 'file'):
            url = None
        else:
            url = '{}/{}'.format(url, target)

        return [{
            'path': dst.relative_to(self.path),
            'url': url,
            'creator': creators,
            'dataset': dataset.name,
            'parent': self
        }]

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

    def dataset_commits(self, dataset, max_results=None):
        """Gets the newest commit for a dataset or its files.

        Commits are returned sorted from newest to oldest.
        """
        paths = [(Path(dataset.path) / self.METADATA).resolve()]

        paths.extend(f.full_path for f in dataset.files)

        commits = self.repo.iter_commits(paths=paths, max_count=max_results)

        return commits

    def add_dataset_tag(self, dataset, tag, description='', force=False):
        """Adds a new tag to a dataset.

        Validates if the tag already exists and that the tag follows
        the same rules as docker tags.
        See https://docs.docker.com/engine/reference/commandline/tag/
        for a documentation of docker tag syntax.

        :raises: ValueError
        """
        if len(tag) > 128:
            raise ValueError('Tags can be at most 128 characters long.')

        if not re.match('^(?![.-])[a-zA-Z0-9_.-]{1,128}$', tag):
            raise ValueError((
                'Tag {} is invalid. \n'
                'Only characters a-z, A-Z, 0-9, ., - and _ '
                'are allowed. \nTag can\'t start with a . or -'
            ).format(tag))

        if any(t for t in dataset.tags if t.name == tag):
            if force:
                # remove duplicate tag
                dataset.tags = [t for t in dataset.tags if t.name != tag]
            else:
                raise ValueError('Tag {} already exists'.format(tag))

        latest_commit = list(self.dataset_commits(dataset, max_results=1))[0]

        tag = DatasetTag(
            name=tag,
            description=description,
            commit=latest_commit.hexsha,
            dataset=dataset.name
        )

        dataset.tags.append(tag)

        return dataset

    def remove_dataset_tags(self, dataset, tags):
        """Removes tags from a dataset."""
        tag_names = {t.name for t in dataset.tags}
        not_found = set(tags).difference(tag_names)

        if len(not_found) > 0:
            raise ValueError('Tags {} not found'.format(', '.join(not_found)))
        dataset.tags = [t for t in dataset.tags if t.name not in tags]

        return dataset


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


DATASET_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS),
    Path(RENKU_HOME) / Path(LinkReference.REFS),
]
