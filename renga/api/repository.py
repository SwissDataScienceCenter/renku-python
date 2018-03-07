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
"""Client for handling a local repository."""

import datetime
import uuid
from contextlib import contextmanager
from subprocess import PIPE, STDOUT, call

import attr
import filelock
import yaml
from git import InvalidGitRepositoryError
from git import Repo as GitRepo
from werkzeug.utils import secure_filename

from renga._compat import Path

HAS_LFS = call(['git', 'lfs'], stdout=PIPE, stderr=STDOUT) == 0


@attr.s
class RepositoryApiMixin(object):
    """Client for handling a local repository."""

    renga_home = attr.ib(default='.renga')
    """Define a name of the Renga folder (default: ``.renga``)."""

    renga_path = attr.ib(init=False)
    """Store a ``Path`` instance of the Renga folder."""

    git = attr.ib(init=False)
    """Store an instance of the Git repository."""

    METADATA = 'metadata.yml'
    """Default name of Renga config file."""

    LOCK_SUFFIX = '.lock'
    """Default suffix for Renga lock file."""

    WORKFLOW = 'workflow'
    """Directory for storing workflow in Renga."""

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        #: Configure Renga path.
        path = Path(self.renga_home)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        self.renga_path = path

        #: Create an instance of a Git repository for the given path.
        try:
            self.git = GitRepo(str(self.path))
        except InvalidGitRepositoryError:
            self.git = None
        # TODO except

    @property
    def lock(self):
        """Create a Renga config lock."""
        return filelock.FileLock(
            str(self.renga_path.with_suffix(self.LOCK_SUFFIX))
        )

    @property
    def renga_metadata_path(self):
        """Return a ``Path`` instance of Renga metadata file."""
        return self.renga_path.joinpath(self.METADATA)

    @property
    def workflow_path(self):
        """Return a ``Path`` instance of the workflow folder."""
        return self.renga_path / self.WORKFLOW

    @contextmanager
    def with_metadata(self):
        """Yield an editable metadata object."""
        with self.lock:
            from renga.models._jsonld import asjsonld
            from renga.models.projects import Project

            path = str(self.renga_metadata_path)

            if self.renga_metadata_path.exists():
                with open(path, 'r') as f:
                    source = yaml.load(f) or {}
                metadata = Project.from_jsonld(source)
            else:
                source = {}
                metadata = Project()

            yield metadata

            source.update(**asjsonld(metadata))
            with open(path, 'w') as f:
                yaml.dump(source, f, default_flow_style=False)

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        with self.lock:
            from renga.models.cwl._ascwl import ascwl
            from renga.models.cwl.workflow import Workflow

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

                with open(workflow_path / step_name, 'w') as step_file:
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

    def init_repository(
        self, name=None, force=False, use_external_storage=True
    ):
        """Initialize a local Renga repository."""
        path = self.path.absolute()
        if force:
            self.renga_path.mkdir(parents=True, exist_ok=force)
            if self.git is None:
                self.git = GitRepo.init(str(path))
        else:
            if self.git is not None:
                raise FileExistsError(self.git.git_dir)

            self.renga_path.mkdir(parents=True, exist_ok=force)
            self.git = GitRepo.init(str(path))

        self.git.description = name or path.name

        # FIXME do not append
        with open(path / '.gitignore', 'a') as gitignore:
            gitignore.write(
                str(
                    self.renga_path.relative_to(self.path)
                    .with_suffix(self.LOCK_SUFFIX)
                ) + '\n'
            )

        with self.with_metadata() as metadata:
            metadata.name = name
            metadata.updated = datetime.datetime.utcnow()

        # initialize LFS if it is requested and installed
        if use_external_storage and HAS_LFS:
            self.init_external_storage(force=force)

        return str(path)

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        cmd = ['git', 'lfs', 'install', '--local']
        if force:
            cmd.append('--force')

        call(
            cmd,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=self.path.absolute(),
        )

    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if HAS_LFS and self.git.config_reader(config_level='repository'
                                              ).has_section('filter "lfs"'):
            call(
                ['git', 'lfs', 'track'] + list(paths),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=self.path,
            )
