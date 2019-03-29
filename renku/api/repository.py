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
"""Client for handling a local repository."""

import datetime
import uuid
from collections import defaultdict
from contextlib import contextmanager
from subprocess import check_output

import attr
import filelock
import yaml
from werkzeug.utils import cached_property, secure_filename

from renku._compat import Path
from renku.models.refs import LinkReference

from ._git import GitCore


def default_path():
    """Return default repository path."""
    from git import InvalidGitRepositoryError
    from renku.cli._git import get_git_home
    try:
        return get_git_home()
    except InvalidGitRepositoryError:
        return '.'


@attr.s
class PathMixin:
    """Define a default path attribute."""

    path = attr.ib(
        default=default_path,
        converter=lambda arg: Path(arg).resolve().absolute(),
    )

    @path.validator
    def _check_path(self, _, value):
        """Check the path exists and it is a directory."""
        if not (value.exists() and value.is_dir()):
            raise ValueError('Define an existing directory.')


@attr.s
class RepositoryApiMixin(GitCore):
    """Client for handling a local repository."""

    renku_home = attr.ib(default='.renku')
    """Define a name of the Renku folder (default: ``.renku``)."""

    renku_path = attr.ib(init=False)
    """Store a ``Path`` instance of the Renku folder."""

    parent = attr.ib(default=None)
    """Store a pointer to the parent repository."""

    METADATA = 'metadata.yml'
    """Default name of Renku config file."""

    LOCK_SUFFIX = '.lock'
    """Default suffix for Renku lock file."""

    WORKFLOW = 'workflow'
    """Directory for storing workflow in Renku."""

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        #: Configure Renku path.
        path = Path(self.renku_home)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        self.renku_path = path

        self._subclients = {}

        super().__attrs_post_init__()

        # initialize submodules
        if self.repo:
            check_output([
                'git', 'submodule', 'update', '--init', '--recursive'
            ],
                         cwd=str(self.path))
        # TODO except

    @property
    def lock(self):
        """Create a Renku config lock."""
        return filelock.FileLock(
            str(self.renku_path.with_suffix(self.LOCK_SUFFIX)),
            timeout=0,
        )

    @property
    def renku_metadata_path(self):
        """Return a ``Path`` instance of Renku metadata file."""
        return self.renku_path.joinpath(self.METADATA)

    @property
    def workflow_path(self):
        """Return a ``Path`` instance of the workflow folder."""
        return self.renku_path / self.WORKFLOW

    @cached_property
    def cwl_prefix(self):
        """Return a CWL prefix."""
        self.workflow_path.mkdir(parents=True, exist_ok=True)  # for Python 3.5
        return str(self.workflow_path.resolve().relative_to(self.path))

    @cached_property
    def project(self):
        """Return FOAF/PROV representation of the project."""
        from renku.cli._docker import GitURL
        from renku.models.provenance import Project

        remote_name = 'origin'
        try:
            remote_branch = self.repo.head.reference.tracking_branch()
            if remote_branch is not None:
                remote_name = remote_branch.remote_name
        except TypeError:
            pass

        try:
            url = GitURL.parse(self.repo.remotes[remote_name].url)

            # Remove gitlab. unless running on gitlab.com.
            hostname_parts = url.hostname.split('.')
            if len(hostname_parts) > 2 and hostname_parts[0] == 'gitlab':
                hostname_parts = hostname_parts[1:]
            url = attr.evolve(url, hostname='.'.join(hostname_parts))
        except IndexError:
            url = None

        if url:
            remote_url = 'https://' + url.hostname

            if url.owner:
                remote_url += '/' + url.owner
            if url.name:
                remote_url += '/' + url.name

            return Project(id=remote_url)

        return Project(id='file://{0}'.format(self.path))

    def process_commit(self, commit=None, path=None):
        """Build an :class:`~renku.models.provenance.activities.Activity` instance.

        :param commit: Commit to process. (default: ``HEAD``)
        :param path: Process a specific CWL file.
        """
        from renku.models.cwl._ascwl import CWLClass
        from renku.models.provenance.activities import Activity

        commit = commit or self.repo.head.commit

        if len(commit.parents) > 1:
            return Activity(commit=commit, client=self)

        if path is None:
            for file_ in commit.stats.files.keys():
                # Find a process (CommandLineTool or Workflow)
                if self.is_cwl(file_):
                    if path is not None:
                        # Regular activity since it edits multiple CWL files
                        return Activity(commit=commit, client=self)

                    path = file_

        if path:
            data = (commit.tree / path).data_stream.read()
            process = CWLClass.from_cwl(
                yaml.safe_load(data), __reference__=Path(path)
            )

            return process.create_run(
                commit=commit,
                client=self,
                process=process,
                path=path,
            )

        return Activity(commit=commit, client=self)

    def is_cwl(self, path):
        """Check if the path is a valid CWL file."""
        return path.startswith(self.cwl_prefix) and path.endswith('.cwl')

    def find_previous_commit(self, paths, revision='HEAD'):
        """Return a previous commit for a given path."""
        file_commits = list(self.repo.iter_commits(revision, paths=paths))

        if not file_commits:
            raise KeyError(
                'Could not find a file {0} in range {1}'.format(
                    paths, revision
                )
            )

        return file_commits[0]

    @cached_property
    def workflow_names(self):
        """Return index of workflow names."""
        names = defaultdict(list)
        for ref in LinkReference.iter_items(self, common_path='workflows'):
            names[str(ref.reference.relative_to(self.path))].append(ref.name)
        return names

    @cached_property
    def submodules(self):
        """Return list of submodules it belongs to."""
        if self.parent:
            client, submodule = self.parent
            return client.submodules + [submodule.name]
        return []

    def subclients(self, parent_commit):
        """Return mapping from submodule to client."""
        if parent_commit in self._subclients:
            return self._subclients[parent_commit]

        try:
            from git import Submodule

            submodules = [
                submodule for submodule in
                Submodule.iter_items(self.repo, parent_commit=parent_commit)
            ]
        except (RuntimeError, ValueError):
            # There are no submodules assiciated with the given commit.
            submodules = []

        return self._subclients.setdefault(
            parent_commit, {
                submodule: self.__class__(
                    path=(self.path / submodule.path).resolve(),
                    parent=(self, submodule),
                )
                for submodule in submodules
            }
        )

    def resolve_in_submodules(self, commit, path):
        """Resolve filename in submodules."""
        original_path = self.path / path
        if original_path.is_symlink() or str(path
                                             ).startswith('.renku/vendors'):
            original_path = original_path.resolve()
            for submodule, subclient in self.subclients(commit).items():
                try:
                    subpath = original_path.relative_to(subclient.path)
                    return (
                        subclient,
                        subclient.find_previous_commit(
                            subpath, revision=submodule.hexsha
                        ),
                        subpath,
                    )
                except ValueError:
                    pass

        return self, commit, path

    @contextmanager
    def with_metadata(self):
        """Yield an editable metadata object."""
        from renku.models.projects import Project

        metadata_path = self.renku_metadata_path

        if metadata_path.exists():
            metadata = Project.from_yaml(metadata_path)
        else:
            metadata = Project.from_jsonld({}, __reference__=metadata_path)

        yield metadata

        metadata.to_yaml()

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        from renku.models.cwl._ascwl import ascwl
        from renku.models.cwl.workflow import Workflow

        workflow = Workflow()
        yield workflow

        for step in workflow.steps:
            step_name = '{0}_{1}.cwl'.format(
                uuid.uuid4().hex,
                secure_filename('_'.join(step.run.baseCommand)),
            )

            workflow_path = self.workflow_path
            if not workflow_path.exists():
                workflow_path.mkdir()

            step_path = workflow_path / step_name
            with step_path.open('w') as step_file:
                yaml.dump(
                    ascwl(
                        # filter=lambda _, x: not (x is False or bool(x)
                        step.run,
                        filter=lambda _, x: x is not None,
                        basedir=workflow_path,
                    ),
                    stream=step_file,
                    default_flow_style=False
                )

    def init_repository(self, name=None, force=False):
        """Initialize a local Renku repository."""
        from git import Repo

        path = self.path.absolute()
        if force:
            self.renku_path.mkdir(parents=True, exist_ok=force)
            if self.repo is None:
                self.repo = Repo.init(str(path))
        else:
            if self.repo is not None:
                raise FileExistsError(self.repo.git_dir)

            self.renku_path.mkdir(parents=True, exist_ok=force)
            self.repo = Repo.init(str(path))

        self.repo.description = name or path.name

        # Check that an author can be determined from Git.
        from renku.models.datasets import Author
        Author.from_git(self.repo)

        # TODO read existing gitignore and create a unique set of rules
        import pkg_resources
        gitignore_default = pkg_resources.resource_stream(
            'renku.data', 'gitignore.default'
        )
        gitignore_path = path / '.gitignore'
        with gitignore_path.open('w') as gitignore:
            gitignore.write(gitignore_default.read().decode())

            gitignore.write(
                '\n' + str(
                    self.renku_path.relative_to(self.path).
                    with_suffix(self.LOCK_SUFFIX)
                ) + '\n'
            )

        with self.with_metadata() as metadata:
            metadata.name = name
            metadata.updated = datetime.datetime.utcnow()

        return str(path)
