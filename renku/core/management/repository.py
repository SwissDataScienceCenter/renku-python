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
import hashlib
import json
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
from renku.core.models.provenance.activity import ActivityCollection
from renku.core.models.provenance.provenance_graph import ProvenanceGraph
from renku.core.models.refs import LinkReference
from renku.core.models.workflow.dependency_graph import DependencyGraph

from .git import GitCore

DEFAULT_DATA_DIR = "data"


def default_path(path="."):
    """Return default repository path."""
    from git import InvalidGitRepositoryError

    from renku.core.commands.git import get_git_home

    try:
        return get_git_home(path=path)
    except InvalidGitRepositoryError:
        return path


def path_converter(path):
    """Converter for path in PathMixin."""
    return Path(path).resolve()


@attr.s
class PathMixin:
    """Define a default path attribute."""

    path = attr.ib(default=default_path, converter=path_converter,)

    @path.validator
    def _check_path(self, _, value):
        """Check the path exists and it is a directory."""
        if not (value.exists() and value.is_dir()):
            raise ValueError("Define an existing directory.")


@attr.s
class RepositoryApiMixin(GitCore):
    """Client for handling a local repository."""

    renku_home = attr.ib(default=RENKU_HOME)
    """Define a name of the Renku folder (default: ``.renku``)."""

    renku_path = attr.ib(init=False)
    """Store a ``Path`` instance of the Renku folder."""

    parent = attr.ib(default=None)
    """Store a pointer to the parent repository."""

    data_dir = attr.ib(
        default=DEFAULT_DATA_DIR, kw_only=True, converter=lambda value: str(value) if value else DEFAULT_DATA_DIR
    )
    """Define a name of the folder for storing datasets."""

    METADATA = "metadata.yml"
    """Default name of Renku config file."""

    LOCK_SUFFIX = ".lock"
    """Default suffix for Renku lock file."""

    WORKFLOW = "workflow"
    """Directory for storing workflow in Renku."""

    DEPENDENCY_GRAPH = "dependency.json"
    """File for storing dependency graph."""

    PROVENANCE_GRAPH = "provenance.json"
    """File for storing ProvenanceGraph."""

    ACTIVITY_INDEX = "activity_index.yaml"
    """Caches activities that generated a path."""

    DOCKERFILE = "Dockerfile"
    """Name of the Dockerfile in the repo."""

    TEMPLATE_CHECKSUMS = "template_checksums.json"

    RENKU_PROTECTED_PATHS = [
        "\\.renku/.*",
        "Dockerfile",
        "\\.dockerignore",
        "\\.gitignore",
        "\\.gitattributes",
        "\\.gitlab-ci\\.yml",
        "environment\\.yml",
        "requirements\\.txt",
    ]

    _commit_activity_cache = attr.ib(factory=dict)

    _activity_index = attr.ib(default=None)

    _remote_cache = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        #: Configure Renku path.
        path = Path(self.renku_home)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        self.renku_path = path

        data_dir = self.get_value("renku", self.DATA_DIR_CONFIG_KEY, local_only=True)
        self.data_dir = data_dir or self.data_dir

        self._subclients = {}

        self._project = None

        super().__attrs_post_init__()

        # initialize submodules
        if self.repo:
            try:
                check_output(["git", "submodule", "update", "--init", "--recursive"], cwd=str(self.path))
            except subprocess.CalledProcessError:
                pass

    @property
    def latest_agent(self):
        """Returns latest agent version used in the repository."""
        try:
            return self.project.agent_version
        except ValueError as e:
            if "Project name not set" in str(e):
                return None
            raise

    @property
    def lock(self):
        """Create a Renku config lock."""
        return filelock.FileLock(str(self.renku_path.with_suffix(self.LOCK_SUFFIX)), timeout=0,)

    @property
    def renku_metadata_path(self):
        """Return a ``Path`` instance of Renku metadata file."""
        return self.renku_path.joinpath(self.METADATA)

    @property
    def workflow_path(self):
        """Return a ``Path`` instance of the workflow folder."""
        return self.renku_path / self.WORKFLOW

    @property
    def activity_index_path(self):
        """Path to the activity filepath cache."""
        return self.renku_path / self.ACTIVITY_INDEX

    @property
    def docker_path(self):
        """Path to the Dockerfile."""
        return self.path / self.DOCKERFILE

    @property
    def template_checksums(self):
        """Return a ``Path`` instance to the template checksums file."""
        return self.renku_path / self.TEMPLATE_CHECKSUMS

    @property
    def provenance_graph_path(self):
        """Path to store activity files."""
        return self.renku_path / self.PROVENANCE_GRAPH

    @property
    def dependency_graph_path(self):
        """Path to the dependency graph file."""
        return self.renku_path / self.DEPENDENCY_GRAPH

    @cached_property
    def cwl_prefix(self):
        """Return a CWL prefix."""
        self.workflow_path.mkdir(parents=True, exist_ok=True)  # for Python 3.5
        return str(self.workflow_path.resolve().relative_to(self.path))

    @property
    def project(self):
        """Return the Project instance."""
        if self.renku_metadata_path.exists() and self._project is None:
            self._project = Project.from_yaml(self.renku_metadata_path, client=self)

        return self._project

    @property
    def remote(self, remote_name="origin"):
        """Return host, owner and name of the remote if it exists."""
        from renku.core.models.git import GitURL

        original_remote_name = remote_name

        if original_remote_name in self._remote_cache:
            return self._remote_cache[original_remote_name]

        host = owner = name = None
        try:
            remote_branch = self.repo.head.reference.tracking_branch()
            if remote_branch is not None:
                remote_name = remote_branch.remote_name
        except TypeError:
            pass

        try:
            url = GitURL.parse(self.repo.remotes[remote_name].url)

            # Remove gitlab. unless running on gitlab.com.
            hostname_parts = url.hostname.split(".")
            if len(hostname_parts) > 2 and hostname_parts[0] == "gitlab":
                hostname_parts = hostname_parts[1:]
            url = attr.evolve(url, hostname=".".join(hostname_parts))
        except IndexError:
            url = None

        if url:
            host = url.hostname
            owner = url.owner
            name = url.name

        remote = {"host": host, "owner": owner, "name": name}
        self._remote_cache[original_remote_name] = remote

        return remote

    def is_project_set(self):
        """Return if project is set for the client."""
        return self._project is not None

    def process_commit(self, commit=None, path=None):
        """Build an :class:`~renku.core.models.provenance.activities.Activity`.

        :param commit: Commit to process. (default: ``HEAD``)
        :param path: Process a specific CWL file.
        """
        from renku.core.models.provenance.activities import Activity

        commit = commit or self.repo.head.commit
        if len(commit.parents) > 1:
            return Activity(commit=commit, client=self)

        if path is None:
            for file_ in commit.stats.files.keys():
                # Find a process (CommandLineTool or Workflow)
                if self.is_workflow(file_):
                    if path is not None:
                        # Regular activity since it edits multiple CWL files
                        return Activity(commit=commit, client=self)

                    path = file_

        if not path:
            # search for activities a file could have been a part of
            activities = self.activities_for_paths(commit.stats.files.keys(), file_commit=commit, revision="HEAD")
            if len(activities) > 1:
                raise errors.CommitProcessingError(
                    "Found multiple activities that produced the same entity at commit {}".format(commit)
                )
            if activities:
                return activities[0]
        else:
            data = (commit.tree / path).data_stream.read()
            process = Activity.from_jsonld(yaml.safe_load(data), client=self, commit=commit)

            return process

        return Activity(commit=commit, client=self)

    def is_workflow(self, path):
        """Check if the path is a valid CWL file."""
        return path.startswith(self.cwl_prefix) and path.endswith(".yaml")

    def find_previous_commit(self, paths, revision="HEAD", return_first=False, full=False):
        """Return a previous commit for a given path starting from ``revision``.

        :param revision: revision to start from, defaults to ``HEAD``
        :param return_first: show the first commit in the history
        :param full: return full history
        :raises KeyError: if path is not present in the given commit
        """
        kwargs = {}

        if full:
            kwargs["full_history"] = True

        if return_first:
            file_commits = list(self.repo.iter_commits(revision, paths=paths, **kwargs))
        else:
            file_commits = list(self.repo.iter_commits(revision, paths=paths, max_count=1, **kwargs))

        if not file_commits:
            raise KeyError("Could not find a file {0} in range {1}".format(paths, revision))

        return file_commits[-1 if return_first else 0]

    @cached_property
    def workflow_names(self):
        """Return index of workflow names."""
        names = defaultdict(list)
        for ref in LinkReference.iter_items(self, common_path="workflows"):
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

            submodules = [submodule for submodule in Submodule.iter_items(self.repo, parent_commit=parent_commit)]
        except (RuntimeError, ValueError):
            # There are no submodules associated with the given commit.
            submodules = []

        subclients = {}
        for submodule in submodules:
            subpath = (self.path / submodule.path).resolve()
            is_renku = subpath / Path(self.renku_home)

            if subpath.exists() and is_renku.exists():
                subclients[submodule] = self.__class__(path=subpath, parent=(self, submodule),)

        return subclients

    def resolve_in_submodules(self, commit, path):
        """Resolve filename in submodules."""
        original_path = self.path / path
        in_vendor = str(path).startswith(".renku/vendors")

        if original_path.is_symlink() or in_vendor:
            original_path = Path(os.path.realpath(os.path.abspath(str(original_path))))

            for submodule, subclient in self.subclients(commit).items():
                if (Path(submodule.path) / Path(".git")).exists():

                    try:
                        subpath = original_path.relative_to(subclient.path)
                        return (
                            subclient,
                            subclient.find_previous_commit(subpath, revision=submodule.hexsha),
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
            if "HEAD is a detached" in str(e):
                current_commit = self.repo.head.commit
            else:
                raise ValueError("Couldn't get active branch or commit", e)

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

        yield metadata

        if not read_only:
            metadata.to_yaml(path=metadata_path)

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        from renku.core.models.cwl.workflow import Workflow

        workflow = Workflow()
        yield workflow

        for step in workflow.steps:
            step_name = "{0}_{1}.yaml".format(uuid.uuid4().hex, secure_filename("_".join(step.run.baseCommand)),)

            workflow_path = self.workflow_path
            if not workflow_path.exists():
                workflow_path.mkdir()

            path = workflow_path / step_name

            run = step.run.generate_process_run(client=self, commit=self.repo.head.commit, path=path,)
            run.to_yaml(path=path)
            self.add_to_activity_index(run)

    def process_and_store_run(self, command_line_tool, name, client):
        """Create Plan and Activity from CommandLineTool and store them."""
        filename = "{0}_{1}.yaml".format(uuid.uuid4().hex, secure_filename("_".join(command_line_tool.baseCommand)))

        # Store Run and ProcessRun as before
        self.workflow_path.mkdir(exist_ok=True)
        path = self.workflow_path / filename

        process_run = command_line_tool.generate_process_run(client=self, commit=self.repo.head.commit, path=path)
        process_run.to_yaml(path=path)
        self.add_to_activity_index(process_run)

        self.update_graphs(process_run)

    def update_graphs(self, activity_run):
        """Update Dependency and Provenance graphs from a ProcessRun/WorkflowRun."""
        if not self.has_graph_files():
            return

        dependency_graph = DependencyGraph.from_json(self.dependency_graph_path)
        provenance_graph = ProvenanceGraph.from_json(self.provenance_graph_path)

        activity_collection = ActivityCollection.from_activity_run(activity_run, dependency_graph, self)

        provenance_graph.add(activity_collection)

        dependency_graph.to_json()
        provenance_graph.to_json()

    def has_graph_files(self):
        """Return true if dependency or provenance graph exists."""
        return self.dependency_graph_path.exists() or self.provenance_graph_path.exists()

    def init_repository(self, force=False, user=None):
        """Initialize an empty Renku repository."""
        from git import Repo

        from renku.core.models.provenance.agents import Person

        # verify if folder is empty
        if self.repo is not None and not force:
            raise errors.InvalidFileOperation(
                "Folder {0} already contains file. Use --force to overwrite".format(self.repo.git_dir)
            )

        # initialize repo and set user data
        path = self.path.absolute()
        self.repo = Repo.init(str(path))
        if user:
            config_writer = self.repo.config_writer()
            for key, value in user.items():
                config_writer.set_value("user", key, value)
            config_writer.release()

        # verify if author information is available
        Person.from_git(self.repo)

    @property
    def path_activity_cache(self):
        """Cache of all activities and their generated paths."""
        # TODO: this is there for performance reasons. Remove once graph
        # is stored as a flat, append-only list (Should be graph query
        # in the future)
        if self._activity_index:
            return self._activity_index

        path = self.activity_index_path

        if path.exists():
            with path.open("r") as stream:
                self._activity_index = yaml.safe_load(stream)
        else:
            self._activity_index = {}

        return self._activity_index

    def add_to_activity_index(self, activity):
        """Add an activity and it's generations to the cache."""
        for g in activity.generated:
            if g.path not in self.path_activity_cache:
                self.path_activity_cache[g.path] = {}
            hexsha = g.commit.hexsha
            if hexsha not in self.path_activity_cache[g.path]:
                self.path_activity_cache[g.path][hexsha] = []

            if activity.path in self.path_activity_cache[g.path][hexsha]:
                continue

            self.path_activity_cache[g.path][g.commit.hexsha].append(activity.path)

        if self.path_activity_cache:
            with self.activity_index_path.open("w") as stream:
                yaml.dump(self.path_activity_cache, stream)

    def activities_for_paths(self, paths, file_commit=None, revision="HEAD"):
        """Get all activities involving a path."""
        from renku.core.models.provenance.activities import Activity

        result = set()

        for p in paths:
            if p not in self.path_activity_cache:
                parent_paths = [a for a in self.path_activity_cache.keys() if p.startswith(a)]
                if not parent_paths:
                    continue
                matching_activities = [
                    a
                    for k, v in self.path_activity_cache.items()
                    for ck, cv in v.items()
                    for a in cv
                    if k in parent_paths and (not file_commit or ck == file_commit.hexsha)
                ]
            else:
                matching = self.path_activity_cache[p]
                if file_commit:
                    if file_commit.hexsha not in matching:
                        continue
                    matching_activities = matching[file_commit.hexsha]
                else:
                    matching_activities = [a for v in matching.values() for a in v]

            for a in matching_activities:
                if (a, revision) in self._commit_activity_cache:
                    activity = self._commit_activity_cache[(a, revision)]
                else:
                    try:
                        commit = self.find_previous_commit(a, revision=revision)
                    except KeyError:
                        continue

                    activity = Activity.from_yaml(self.path / a, client=self, commit=commit)

                    self._commit_activity_cache[(a, revision)] = activity
                result.add(activity)

        return list(result)

    def import_from_template(self, template_path, metadata, force=False):
        """Render template files from a template directory."""
        checksums = {}
        for file in template_path.glob("**/*"):
            rel_path = file.relative_to(template_path)
            destination = self.path / rel_path
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
                # NOTE: the path could contain template variables, we need to template it
                destination = Path(Template(str(destination)).render(metadata))
                destination.write_text(rendered_content)
                checksums[str(rel_path)] = self._content_hash(destination)
            except IsADirectoryError:
                destination.mkdir(parents=True, exist_ok=True)
            except TypeError:
                shutil.copy(file, destination)

        self.template_checksums.parent.mkdir(parents=True, exist_ok=True)

        with open(self.template_checksums, "w") as checksum_file:
            json.dump(checksums, checksum_file)

    def check_immutable_template_files(self, *paths):
        """Check paths and return a list of those that are marked immutable in the project template."""
        if not self.project.immutable_template_files:
            return []

        immutable_template_files = self.project.immutable_template_files or []
        return [p for p in paths if str(p) in immutable_template_files]

    def _content_hash(self, path):
        """Calculate the sha256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(str(path), "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
