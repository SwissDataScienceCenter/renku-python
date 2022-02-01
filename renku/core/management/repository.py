# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
import shutil
from contextlib import contextmanager
from uuid import uuid4

import attr
import filelock
from jinja2 import Template

from renku.core import errors
from renku.core.compat import Path
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder import inject
from renku.core.management.git import GitCore
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.models.enums import ConfigFilter
from renku.core.models.project import Project
from renku.core.utils import communication
from renku.core.utils.git import default_path

DEFAULT_DATA_DIR = "data"

INIT_APPEND_FILES = [".gitignore"]
INIT_KEEP_FILES = ["readme.md", "readme.rst"]


def path_converter(path):
    """Converter for path in PathMixin."""
    return Path(path).resolve()


@attr.s
class PathMixin:
    """Define a default path attribute."""

    path = attr.ib(default=default_path, converter=path_converter)

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

    LOCK_SUFFIX = ".lock"
    """Default suffix for Renku lock file."""

    DATABASE_PATH: str = "metadata"
    """Directory for metadata storage."""

    DOCKERFILE = "Dockerfile"
    """Name of the Dockerfile in the repository."""

    TEMPLATE_CHECKSUMS = "template_checksums.json"

    RENKU_PROTECTED_PATHS = [
        ".dockerignore",
        ".git",
        ".git/*",
        ".gitattributes",
        ".gitignore",
        ".gitlab-ci.yml",
        ".renku",
        ".renku/*",
        ".renkulfsignore",
        "Dockerfile",
        "environment.yml",
        "requirements.txt",
    ]

    _remote_cache = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        #: Configure Renku path.
        path = Path(self.renku_home)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        self.renku_path = path

        data_dir = self.get_value("renku", self.DATA_DIR_CONFIG_KEY, config_filter=ConfigFilter.LOCAL_ONLY)
        self.data_dir = data_dir or self.data_dir

        self._project = None

        self._transaction_id = None

        super().__attrs_post_init__()

        # initialize submodules
        if self.repository:
            try:
                self.repository.run_git_command("submodule", "update", "--init", "--recursive", cwd=str(self.path))
            except errors.GitCommandError:
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
        return filelock.FileLock(str(self.renku_path.with_suffix(self.LOCK_SUFFIX)), timeout=0)

    @property
    def docker_path(self):
        """Path to the Dockerfile."""
        return self.path / self.DOCKERFILE

    @property
    def template_checksums(self):
        """Return a ``Path`` instance to the template checksums file."""
        return self.renku_path / self.TEMPLATE_CHECKSUMS

    @property
    def database_path(self) -> Path:
        """Path to the metadata storage directory."""
        return self.renku_path / self.DATABASE_PATH

    @property
    @inject.autoparams()
    def project(self, project_gateway: IProjectGateway):
        """Return the Project instance."""
        if self._project is None:
            self._project = project_gateway.get_project()

        return self._project

    @property
    def transaction_id(self):
        """Get a transaction id for the current client to be used for grouping git commits."""
        if not self._transaction_id:
            self._transaction_id = uuid4().hex

        return f"\n\nrenku-transaction: {self._transaction_id}"

    @property
    def remote(self, remote_name="origin"):
        """Return host, owner and name of the remote if it exists."""
        from renku.core.models.git import GitURL

        original_remote_name = remote_name

        if original_remote_name in self._remote_cache:
            return self._remote_cache[original_remote_name]

        host = owner = name = None
        try:
            remote_branch = self.repository.active_branch.remote_branch
            if remote_branch is not None:
                remote_name = remote_branch.remote.name
        except (AttributeError, errors.GitError):
            # NOTE: AttributeError is raised if there is a None value
            pass

        try:
            url = GitURL.parse(self.repository.remotes[remote_name].url)

            # Remove gitlab. unless running on gitlab.com.
            hostname_parts = url.hostname.split(".")
            if len(hostname_parts) > 2 and hostname_parts[0] == "gitlab":
                hostname_parts = hostname_parts[1:]
            url = attr.evolve(url, hostname=".".join(hostname_parts))
        except errors.GitRemoteNotFoundError:
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

    def get_in_submodules(self, commit, path):
        """Resolve filename in submodules."""
        from renku.core.management.client import LocalClient

        original_path = self.path / path
        in_vendor = str(path).startswith(".renku/vendors")

        if original_path.is_symlink() or in_vendor:
            resolved_path = original_path.resolve()

            for submodule in self.repository.submodules:
                if not (submodule.path / ".git").exists():
                    continue

                try:
                    path_within_submodule = resolved_path.relative_to(submodule.path)
                    commit = submodule.get_previous_commit(path=path_within_submodule, revision=commit.hexsha)
                    subclient = LocalClient(submodule.path)
                except (ValueError, errors.GitCommitNotFoundError):
                    pass
                else:
                    return subclient, commit, path_within_submodule

        return self, commit, path

    @contextmanager
    @inject.autoparams()
    def with_metadata(
        self,
        project_gateway: IProjectGateway,
        database_gateway: IDatabaseGateway,
        read_only=False,
        name=None,
        description=None,
        keywords=None,
        custom_metadata=None,
    ):
        """Yield an editable metadata object."""

        try:
            project = project_gateway.get_project()
        except ValueError:
            project = Project.from_client(
                name=name, description=description, keywords=keywords, custom_metadata=custom_metadata, client=self
            )

        yield project

        if not read_only:
            project_gateway.update_project(project)
            database_gateway.commit()

    def has_graph_files(self):
        """Return true if database exists."""
        return self.database_path.exists() and any(
            f for f in self.database_path.iterdir() if f != self.database_path / "root"
        )

    def init_repository(self, force=False, user=None, initial_branch=None):
        """Initialize an empty Renku repository."""
        from renku.core.metadata.repository import Repository

        # initialize repo and set user data
        path = self.path.absolute()
        if force and (path / RENKU_HOME).exists():
            shutil.rmtree(path / RENKU_HOME)
        self.repository = Repository.initialize(path=path, branch=initial_branch)
        if user:
            with self.repository.get_configuration(writable=True) as config_writer:
                for key, value in user.items():
                    config_writer.set_value("user", key, value)

        # verify if git user information is available
        _ = self.repository.get_user()

    def get_template_files(self, template_path, metadata):
        """Gets paths in a rendered renku template."""
        for file in template_path.glob("**/*"):
            rel_path = file.relative_to(template_path)
            destination = self.path / rel_path

            destination = Path(Template(str(destination)).render(metadata))
            yield destination.relative_to(self.path)

    def import_from_template(self, template_path, metadata, force=False):
        """Render template files from a template directory."""
        checksums = {}
        for file in sorted(template_path.glob("**/*")):
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
                templated_rel_path = destination.relative_to(self.path)

                if destination.exists() and str(templated_rel_path).lower() in INIT_APPEND_FILES:
                    communication.echo(f"Appending to file {templated_rel_path} ...")
                    destination.write_text(destination.read_text() + "\n" + rendered_content)
                elif not destination.exists() or str(templated_rel_path).lower() not in INIT_KEEP_FILES:
                    if destination.exists():
                        communication.echo(f"Overwriting file {templated_rel_path} ...")
                    else:
                        communication.echo(f"Initializing file {templated_rel_path} ...")

                    destination.write_text(rendered_content)

                checksums[str(rel_path)] = self._content_hash(destination)
            except IsADirectoryError:
                destination.mkdir(parents=True, exist_ok=True)
            except TypeError:
                shutil.copy(file, destination)

        self.template_checksums.parent.mkdir(parents=True, exist_ok=True)

        with open(self.template_checksums, "w") as checksum_file:
            json.dump(checksums, checksum_file)

    def _content_hash(self, path):
        """Calculate the sha256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(str(path), "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


DATABASE_METADATA_PATH = [
    Path(RENKU_HOME) / RepositoryApiMixin.DATABASE_PATH,
]
