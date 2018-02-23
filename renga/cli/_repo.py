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
"""Renga repository."""

import datetime
import os
import shlex
import uuid
from contextlib import contextmanager
from subprocess import PIPE, STDOUT, call

import attr
import click
import filelock
import yaml
from git import Repo as GitRepo
from werkzeug.utils import secure_filename

from renga._compat import Path
from renga.models.cwl._ascwl import ascwl
from renga.models.cwl.workflow import Workflow

from ._config import RENGA_HOME, read_config, write_config
from ._git import get_git_home

HAS_LFS = not call(['git', 'lfs'], stdout=PIPE, stderr=STDOUT)


def uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, uuid_representer)


class Repo(object):
    """Represent a Renga repository."""

    METADATA = 'metadata.yml'
    """Default name of Renga config file."""

    LOCK_SUFFIX = '.lock'
    """Default suffix for Renga lock file."""

    WORKFLOW = 'workflow'
    """Directory for storing workflow in Renga."""

    def __init__(self, renga=None, git_home=None):
        """Store core options."""
        self.renga_path = renga or RENGA_HOME
        self._git_home = git_home

    @property
    def path(self):
        """Return a ``Path`` instance of this repository."""
        return Path(self._git_home or get_git_home())

    @property
    def git(self):
        """Return a Git repository."""
        return GitRepo(str(self.path))

    @property
    def renga_path(self):
        """Return a ``Path`` instance of Renga folder."""
        path = Path(self._renga_path)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        return path

    @renga_path.setter
    def renga_path(self, value):
        """Update path of the Renga folder."""
        self._renga_path = value

    @property
    def lock(self):
        """Create a Renga config lock."""
        return filelock.FileLock(
            str(self.renga_path.with_suffix(self.LOCK_SUFFIX)))

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
    def with_dataset(self, name=None, datadir='data'):
        """Yield an editable metadata object for a dataset."""
        with self.lock:
            from renga.models._jsonld import asjsonld
            from renga.models.dataset import Dataset
            path = None
            dataset = None

            if name:
                path = self.path / datadir / name / 'metadata.yml'
                if path.exists():
                    with open(path, 'r') as f:
                        source = yaml.load(f) or {}
                    dataset = Dataset.from_jsonld(source)
                    # TODO update? dataset ...
            if dataset is None:
                source = {}
                dataset = Dataset.create(name=name, repo=self.git)

            dataset.repo = self.git
            yield dataset

            source.update(**asjsonld(
                dataset,
                filter=lambda attr, _: attr.name not in {'repo', 'datadir'},
            ))

            # TODO
            # if path is None:
            #     path = self.path / datadir / dataset.name / 'metadata.yml'
            #     if path.exists():
            #         raise ValueError('Dataset already exists')

            with open(path, 'w') as f:
                yaml.dump(source, f, default_flow_style=False)

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        with self.lock:
            workflow = Workflow()
            yield workflow

            for step in workflow.steps:
                step_name = '{0}_{1}.cwl'.format(
                    uuid.uuid4().hex,
                    secure_filename('_'.join(step.run.baseCommand)), )

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
                        default_flow_style=False)

    def init(self, name=None, force=False, lfs=True):
        """Initialize a Renga repository."""
        self.renga_path.mkdir(parents=True, exist_ok=force)

        path = self.path.absolute()
        if force:
            try:
                git = GitRepo.init(str(path))
            except FileExistsError:
                git = GitRepo(str(path))
        else:
            git = GitRepo.init(str(path))

        git.description = name or path.name

        with open(path / '.gitignore', 'a') as gitignore:
            gitignore.write(
                str(
                    self.renga_path.relative_to(self.path).with_suffix(
                        self.LOCK_SUFFIX)) + '\n')

        with self.with_metadata() as metadata:
            metadata.name = name
            metadata.updated = datetime.datetime.utcnow()

        # initialize LFS if it is requested and installed
        if lfs and HAS_LFS:
            self.init_lfs()

        return str(path)

    def init_lfs(self):
        """Initialize the git-LFS."""
        path = self.path.absolute()

        call(['git', 'lfs', 'install', '--local'], stdout=PIPE, stderr=STDOUT)

        # track everything in ./data except for metadata.json files
        with open(path / '.gitattributes', 'w') as gitattributes:
            gitattributes.write('\n'.join([
                'data/** filter=lfs diff=lfs merge=lfs -text',
                'data/**/metadata.json -filter=lfs -diff=lfs -merge=lfs -text'
            ]) + '\n')

    def track_lfs_paths(self, paths):
        """Track paths in LFS."""
        if HAS_LFS:
            p = call(
                ['git', 'lfs', 'track', ' '.join(paths)],
                stdout=PIPE,
                stderr=STDOUT)

    @property
    def state(self):
        """Return the current state object."""
        raise NotImplemented()  # pragma: no cover


pass_repo = click.make_pass_decorator(Repo, ensure=True)
