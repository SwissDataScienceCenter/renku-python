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
"""Client for handling a local repository."""
import os
import shutil
import subprocess
import uuid
from collections import defaultdict
from contextlib import contextmanager
from subprocess import check_output

import attr
import filelock
import yaml
from jinja2 import Template
from werkzeug.utils import cached_property, secure_filename

from renku.core import errors
from renku.core.compat import Path
from renku.core.management.config import RENKU_HOME
from renku.core.models.projects import Project
from renku.core.models.refs import LinkReference

from .git import GitCore


def default_path():
    """Return default repository path."""
    from git import InvalidGitRepositoryError
    from renku.core.commands.git import get_git_home
    try:
        return get_git_home()
    except InvalidGitRepositoryError:
        return '.'


def path_converter(path):
    """Converter for path in PathMixin."""
    return Path(path).resolve()


@attr.s
class PathMixin:
    """Define a default path attribute."""

    path = attr.ib(
        default=default_path,
        converter=path_converter,
    )

    @path.validator
    def _check_path(self, _, value):
        """Check the path exists and it is a directory."""
        if not (value.exists() and value.is_dir()):
            raise ValueError('Define an existing directory.')


@attr.s
class RepositoryApiMixin(GitCore):
    """Client for handling a local repository."""

    renku_home = attr.ib(default=RENKU_HOME)
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

    RENKU_PROTECTED_PATHS = [
        '\\.renku/.*', 'Dockerfile', '\\.dockerignore', '\\.gitignore',
        '\\.gitattributes', '\\.gitlab-ci\\.yml', 'environment\\.yml',
        'requirements\\.txt'
    ]

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
            try:
                check_output([
                    'git', 'submodule', 'update', '--init', '--recursive'
                ],
                             cwd=str(self.path))
            except subprocess.CalledProcessError:
                pass

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
        """Return the Project instance."""
        if self.renku_metadata_path.exists():
            return Project.from_yaml(self.renku_metadata_path, client=self)

    @property
    def remote(self, remote_name='origin'):
        """Return host, owner and name of the remote if it exists."""
        from renku.core.models.git import GitURL

        host = owner = name = None
        try:
            remote_branch = \
                self.repo.head.reference.tracking_branch()
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
            host = url.hostname
            owner = url.owner
            name = url.name
        return {'host': host, 'owner': owner, 'name': name}

    def process_commit(self, commit=None, path=None):
        """Build an :class:`~renku.core.models.provenance.activities.Activity`.

        :param commit: Commit to process. (default: ``HEAD``)
        :param path: Process a specific CWL file.
        """
        from renku.core.models.cwl.ascwl import CWLClass
        from renku.core.models.provenance.activities import Activity

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

    def find_previous_commit(self, paths, revision='HEAD', return_first=False):
        """Return a previous commit for a given path starting from ``revision``.

        :param revision: revision to start from, defaults to ``HEAD``
        :param return_first: show the first commit in the history
        :raises KeyError: if path is not present in the given commit
        """
        file_commits = list(self.repo.iter_commits(revision, paths=paths))

        if not file_commits:
            raise KeyError(
                'Could not find a file {0} in range {1}'.format(
                    paths, revision
                )
            )

        return file_commits[-1 if return_first else 0]

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
            # There are no submodules associated with the given commit.
            submodules = []

        subclients = {}
        for submodule in submodules:
            subpath = (self.path / submodule.path).resolve()
            is_renku = subpath / Path(self.renku_home)

            if subpath.exists() and is_renku.exists():
                subclients[submodule] = self.__class__(
                    path=subpath,
                    parent=(self, submodule),
                )

        return subclients

    def resolve_in_submodules(self, commit, path):
        """Resolve filename in submodules."""
        original_path = self.path / path
        in_vendor = str(path).startswith('.renku/vendors')

        if original_path.is_symlink() or in_vendor:
            original_path = Path(
                os.path.realpath(os.path.abspath(str(original_path)))
            )

            for submodule, subclient in self.subclients(commit).items():
                if (Path(submodule.path) / Path('.git')).exists():

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
    def with_commit(self, commit):
        """Yield the state of the repo at a specific commit."""
        current_branch = None
        current_commit = None

        try:
            current_branch = self.repo.active_branch
        except TypeError as e:
            # not on a branch, detached head
            if 'HEAD is a detached' in str(e):
                current_commit = self.repo.head.commit
            else:
                raise ValueError('Couldn\'t get active branch or commit', e)

        self.repo.git.checkout(commit)

        try:
            yield
        finally:
            if current_branch:
                self.repo.git.checkout(current_branch)
            elif current_commit:
                self.repo.git.checkout(current_commit)

    @contextmanager
    def with_metadata(self, read_only=False, name=None):
        """Yield an editable metadata object."""
        metadata_path = self.renku_metadata_path

        if metadata_path.exists():
            metadata = Project.from_yaml(metadata_path, client=self)
        else:
            metadata = Project(name=name, client=self)
            metadata.__reference__ = metadata_path

        yield metadata

        if not read_only:
            metadata.to_yaml()

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        from renku.core.models.cwl.ascwl import ascwl
        from renku.core.models.cwl.workflow import Workflow

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

    def init_repository(self, force=False):
        """Initialize an empty Renku repository."""
        from git import Repo
        from renku.core.models.provenance.agents import Person

        # verify if folder is empty
        if self.repo is not None and not force:
            raise errors.InvalidFileOperation(
                'Folder {0} already contains file. Use --force to overwrite'.
                format(self.repo.git_dir)
            )

        # initialize repo
        path = self.path.absolute()
        self.repo = Repo.init(str(path))

        # verify if author information is available
        Person.from_git(self.repo)

    def import_from_template(self, template_path, metadata, force=False):
        """Render template files from a template directory."""
        for file in template_path.glob('**/*'):
            destination = self.path / file.relative_to(template_path)
            try:
                # TODO: notify about the expected variables - code stub:
                # with file.open() as fr:
                #     file_content = fr.read()
                #     # look for the required keys
                #     env = Environment()
                #     parsed = env.parse(file_content)
                #     variables = meta.find_undeclared_variables(parsed)

                # parse file and process it
                template = Template(file.read_text())
                rendered_content = template.render(metadata)
                destination = Path(Template(str(destination)).render(metadata))
                destination.write_text(rendered_content)
            except IsADirectoryError:
                destination.mkdir(parents=True, exist_ok=True)
            except TypeError:
                shutil.copy(file, destination)
