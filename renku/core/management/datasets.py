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
import tempfile
import uuid
import warnings
from configparser import NoSectionError
from contextlib import contextmanager
from pathlib import Path
from subprocess import PIPE, SubprocessError, run
from urllib import error, parse

import attr
import requests

from renku.core import errors
from renku.core.management.config import RENKU_HOME
from renku.core.models.datasets import Creator, Dataset, DatasetFile, \
    DatasetTag
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
    def with_dataset(self, name=None, identifier=None):
        """Yield an editable metadata object for a dataset."""
        dataset = self.load_dataset(name=name)
        clean_up_required = False

        if dataset is None:
            clean_up_required = True
            dataset_ref = None
            identifier = str(uuid.uuid4())
            path = (self.renku_datasets_path / identifier / self.METADATA)
            try:
                path.parent.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                raise errors.DatasetExistsError(
                    'Dataset with reference {} exists'.format(path.parent)
                )

            with with_reference(path):
                dataset = Dataset(
                    identifier=identifier, name=name, client=self
                )

            if name:
                dataset_ref = LinkReference.create(
                    client=self, name='datasets/' + name
                )
                dataset_ref.set_reference(path)

        dataset_path = self.path / self.datadir / dataset.name
        dataset_path.mkdir(parents=True, exist_ok=True)

        try:
            yield dataset
        except Exception:
            # TODO use a general clean-up strategy
            # https://github.com/SwissDataScienceCenter/renku-python/issues/736
            if clean_up_required and dataset_ref:
                dataset_ref.delete()
                shutil.rmtree(path.parent, ignore_errors=True)
            raise

        # TODO
        # if path is None:
        #     path = dataset_path / self.METADATA
        #     if path.exists():
        #         raise ValueError('Dataset already exists')

        dataset.to_yaml()

    def add_data_to_dataset(
        self,
        dataset,
        urls,
        git=False,
        force=False,
        sources=(),
        destination='',
        link=False
    ):
        """Import the data into the data directory."""
        dataset_path = self.path / self.datadir / dataset.name

        files = []

        for url in urls:
            git = git or check_for_git_repo(url)

            if git:
                sources = sources or ()
                files.extend(
                    self._add_from_git(
                        dataset, dataset_path, url, sources, destination
                    )
                )
            else:
                files.extend(
                    self._add_from_url(
                        dataset, dataset_path, url, link, destination
                    )
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
            if os.path.basename(str(data['path'])) == '.git':
                continue

            datasetfile = DatasetFile.from_revision(self, **data)

            # Set dataset file path relative to projects root for submodules
            if datasetfile.client != self:
                datasetfile.path = str(data['path'])
            dataset_files.append(datasetfile)
        dataset.update_files(dataset_files)

    def _add_from_url(self, dataset, dataset_path, url, link, destination):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme)
            )

        if destination:
            destination = self._resolve_paths(dataset_path,
                                              [destination]).pop()

        dst = self.path / dataset_path / destination

        if dst.exists() and dst.is_dir():
            dst = dst / Path(u.path).name

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            # if we have a directory, recurse
            if src.is_dir():
                if dst.exists() and not dst.is_dir():
                    raise errors.InvalidFileOperation(
                        'Cannot copy directory to a file'
                    )

                files = []
                dst.mkdir(parents=True, exist_ok=True)
                for f in src.iterdir():
                    files.extend(
                        self._add_from_url(
                            dataset,
                            dataset_path,
                            f.absolute().as_posix(),
                            link=link,
                            destination=dst
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

    def _add_from_git(self, dataset, dataset_path, url, sources, destination):
        """Process adding resources from another git repository."""
        from git import Repo
        from renku import LocalClient

        u = parse.urlparse(url)

        if u.scheme in ('', 'file') and not url.startswith('git@'):
            try:
                Path(u.path).resolve().relative_to(self.path)
            except ValueError:
                pass
            else:
                if not destination:
                    return [{
                        'path': url,
                        'url': url,
                        'creator': dataset.creator,
                        'dataset': dataset.name,
                        'parent': self
                    }]

            warnings.warn('Importing local git repository, use HTTPS')

            # determine where is the base repo path
            repo = Repo(url, search_parent_directories=True)
            repo_path = Path(repo.git_dir).parent.resolve()

            # if repo path is a parent of the url, treat the url as a source
            if repo_path != Path(u.path):
                if sources:
                    raise errors.UsageError(
                        'Cannot use --source within local repo subdirectories'
                    )
                source = Path(u.path).resolve().relative_to(repo_path)
                sources = (source, )
                url = repo_path.as_posix()
        elif u.scheme in {'http', 'https', 'git+https', 'git+ssh'} or \
                url.startswith('git@'):
            repo_path = Path(tempfile.mkdtemp())
            repo = Repo.clone_from(url, repo_path, recursive=True)
        else:
            raise NotImplementedError(
                'Scheme {} not supported'.format(u.scheme)
            )

        dataset_path = self.path / dataset_path

        sources = self._resolve_paths(u.path, sources)
        destination = destination or Path('.')
        destination = self._resolve_paths(dataset_path, [destination]).pop()

        dst_root = self.path / dataset_path / destination

        # Get all files from repo that match sources
        copied_sources = set()
        files = set()
        for file in repo.head.commit.tree.traverse():
            path = file.path
            result = self._get_src_and_dst(path, repo_path, sources, dst_root)

            if result:
                files.add(result)
                source = result[3]
                copied_sources.add(source)

        uncopied_sources = sources - copied_sources
        if uncopied_sources:
            uncopied_sources = {str(s) for s in uncopied_sources}
            raise errors.ParameterError(
                'No such file or directory', param_hint=uncopied_sources
            )

        # Create metadata and move files to dataset
        results = []
        client = LocalClient(repo_path)

        paths = set()
        for path, src, _, __ in files:
            if src.is_dir():
                continue
            if src.is_symlink():
                path = str(src.resolve().relative_to(repo_path))
            paths.add(path)
        self._fetch_lfs_files(str(repo_path), paths)

        for path, src, dst, _ in files:
            if not src.is_dir():
                creators = []
                # grab all the creators from the commit history
                for commit in repo.iter_commits(paths=path):
                    creator = Creator.from_commit(commit)
                    if creator not in creators:
                        creators.append(creator)

                if u.scheme in ('', 'file'):
                    dst_url = None
                elif path:
                    dst_url = '{}/{}'.format(url, path)
                else:
                    dst_url = url

                base = DatasetFile.from_revision(
                    client, path=path, url=url, creator=creators
                )

                results.append({
                    'path': dst.relative_to(self.path),
                    'url': dst_url,
                    'creator': creators,
                    'dataset': dataset.name,
                    'parent': self,
                    'based_on': base
                })

                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(str(src), str(dst))

        return results

    def _resolve_paths(self, root_path, paths):
        """Check if paths are within a root path and resolve them."""
        results = set()

        for p in paths:
            try:
                root_path = Path(root_path).resolve()
                path = (root_path / p).resolve().relative_to(root_path)
            except ValueError:
                raise errors.InvalidFileOperation(
                    'File {} is not within path {}'.format(p, root_path)
                )
            else:
                results.add(path)

        return results

    def _get_src_and_dst(self, path, repo_path, sources, dst_root):
        if not sources:
            source = Path('.')
        else:
            source = None
            for s in sources:
                try:
                    Path(path).relative_to(s)
                except ValueError:
                    pass
                else:
                    source = s
                    break

            if not source:
                return

        src = repo_path / path
        source_name = Path(source).name
        relative_path = Path(path).relative_to(source)

        if not dst_root.exists():
            if len(sources) == 1:
                dst = dst_root / relative_path
            else:  # Treat destination as a directory
                dst = dst_root / source_name / relative_path
        elif dst_root.is_dir():
            dst = dst_root / source_name / relative_path
        else:  # Destination is an existing file
            if len(sources) == 1 and not src.is_dir():
                dst = dst_root
            elif not sources:
                raise errors.InvalidFileOperation('Cannot copy repo to file')
            else:
                raise errors.InvalidFileOperation(
                    'Cannot copy multiple files or directories to a file'
                )

        return (path, src, dst, source)

    def _fetch_lfs_files(self, repo_path, paths):
        """Fetch and checkout paths that are tracked by Git LFS."""
        try:
            output = run(('git', 'lfs', 'ls-files', '--name-only'),
                         stdout=PIPE,
                         cwd=repo_path,
                         universal_newlines=True)
        except SubprocessError:
            return

        lfs_files = set(output.stdout.split('\n'))
        files = lfs_files & paths
        if not files:
            return

        try:
            for path in files:
                run(['git', 'lfs', 'pull', '--include', path], cwd=repo_path)
        except KeyboardInterrupt:
            raise
        except SubprocessError:
            pass

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
