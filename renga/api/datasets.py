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
"""Client for handling datasets."""

import os
from urllib import error, parse

import git
import requests
import shutil
import stat

from renga._compat import Path
from renga.models.datasets import Author, Dataset, DatasetFile, NoneType


class DatasetsApiMixin(object):
    """Client for handling datasets."""

    def add_data_to_dataset(
        self, dataset, url, datadir=None, git=False, **kwargs
    ):
        """Import the data into the data directory."""
        datadir = datadir or dataset.datadir
        git = git or check_for_git_repo(url)

        target = kwargs.get('target')

        if git:
            if isinstance(target, (str, NoneType)):
                dataset.files.update(
                    self._add_from_git(dataset, dataset.path, url, target)
                )
            else:
                for t in target:
                    dataset.files.update(
                        self._add_from_git(dataset, dataset.path, url, t)
                    )
        else:
            dataset.files.update(
                self._add_from_url(dataset, dataset.path, url, **kwargs)
            )

    def _add_from_url(self, dataset, path, url, nocopy=False, **kwargs):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme)
            )

        dst = path.joinpath(os.path.basename(url)).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            # if we have a directory, recurse
            if src.is_dir():
                files = {}
                os.mkdir(dst)
                for f in src.iterdir():
                    files.update(
                        self._add_from_url(
                            dataset,
                            dst,
                            f.absolute().as_posix(),
                            nocopy=nocopy
                        )
                    )
                return files
            if nocopy:
                try:
                    os.link(src, dst)
                except Exception as e:
                    raise Exception(
                        'Could not create hard link '
                        '- retry without nocopy.'
                    ) from e
            else:
                shutil.copy(src, dst)

        else:
            try:
                response = requests.get(url)
                dst.write_bytes(response.content)
            except error.HTTPError as e:  # pragma nocover
                raise e

        # make the added file read-only
        mode = dst.stat().st_mode & 0o777
        dst.chmod(mode & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))

        self.track_paths_in_storage(dst.relative_to(self.path))
        result = dst.relative_to(self.path / dataset.path).as_posix()
        return {
            result:
                DatasetFile(
                    result, url, authors=dataset.authors, dataset=dataset.name
                )
        }

    def _add_from_git(self, dataset, path, url, target):
        """Process adding resources from another git repository.

        The submodules are placed in .renga/vendors and linked
        to the *path* specified by the user.
        """
        # create the submodule
        u = parse.urlparse(url)
        submodule_path = self.renga_path / 'vendors' / (u.netloc or 'local')

        if u.scheme in ('', 'file'):
            # determine where is the base repo path
            r = git.Repo(url, search_parent_directories=True)
            src_repo_path = Path(r.git_dir).parent
            submodule_name = os.path.basename(src_repo_path)
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
        elif u.scheme in ('http', 'https'):
            submodule_name = os.path.splitext(os.path.basename(u.path))[0]
            submodule_path = submodule_path.joinpath(
                os.path.dirname(u.path).lstrip('/'), submodule_name
            )
        else:
            raise NotImplementedError(
                'Scheme {} not supported'.format(u.scheme)
            )

        # FIXME: do a proper check that the repos are not the same
        if submodule_name not in (s.name for s in self.git.submodules):
            # new submodule to add
            self.git.create_submodule(
                name=submodule_name, path=submodule_path.as_posix(), url=url
            )

        # link the target into the data directory
        dst = self.path / dataset.path / submodule_name / (target or '')
        src = submodule_path / (target or '')

        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)
        # if we have a directory, recurse
        if src.is_dir():
            files = {}
            os.mkdir(dst)
            for f in src.iterdir():
                files.update(
                    self._add_from_git(
                        dataset,
                        path,
                        url,
                        target=f.relative_to(submodule_path)
                    )
                )
            return files

        os.symlink(os.path.relpath(src, dst.parent), dst)

        # grab all the authors from the commit history
        git_repo = git.Repo(submodule_path.absolute().as_posix())
        authors = set(
            Author(name=commit.author.name, email=commit.author.email)
            for commit in git_repo.iter_commits(paths=target)
        )

        result = dst.relative_to(self.path / dataset.path).as_posix()
        return {
            result:
                DatasetFile(
                    result, '{}/{}'.format(url, target), authors=authors
                )
        }


def check_for_git_repo(url):
    """Check if a url points to a git repository."""
    u = parse.urlparse(url)
    is_git = False

    if os.path.splitext(u.path)[1] == '.git':
        is_git = True
    elif u.scheme in ('', 'file'):
        try:
            git.Repo(u.path, search_parent_directories=True)
            is_git = True
        except git.InvalidGitRepositoryError:
            is_git = False
    return is_git
