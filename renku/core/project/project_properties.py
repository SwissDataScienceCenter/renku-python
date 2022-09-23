# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Project properties/configuration."""

import contextlib
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, NamedTuple, Optional, Union

from renku.core import errors
from renku.core.constant import (
    APP_NAME,
    CONFIG_NAME,
    DATA_DIR_CONFIG_KEY,
    DATABASE_PATH,
    DATASET_IMAGES,
    DEFAULT_DATA_DIR,
    DOCKERFILE,
    LOCK_SUFFIX,
    POINTERS,
    RENKU_HOME,
    TEMPLATE_CHECKSUMS,
)

if TYPE_CHECKING:
    from renku.domain_model.project import Project
    from renku.infrastructure.database import Database
    from renku.infrastructure.repository import Remote, Repository


@dataclass
class ProjectContext:
    """Various properties of the current project."""

    path: Path
    database: Optional["Database"] = None
    datadir: Optional[str] = None
    project: Optional["Project"] = None
    repository: Optional["Repository"] = None
    save_changes: bool = False
    transaction_id: Optional[str] = None


class ProjectProperties(threading.local):
    """A configuration class to hold global configuration."""

    external_storage_requested = True
    """External storage (e.g. LFS) requested for Renku command."""

    def __init__(self) -> None:
        self._context_stack: List[ProjectContext] = []

    def __del__(self):
        self.clear()

    @property
    def database(self) -> "Database":
        """Current database."""
        if not self._top.database:
            from renku.infrastructure.database import Database

            self._top.database = Database.from_path(self.database_path)

        return self._top.database

    @property
    def database_path(self) -> Path:
        """Path to the metadata storage directory."""
        return self.metadata_path / DATABASE_PATH

    @property
    def datadir(self) -> str:
        """Define a name of the folder for storing datasets."""
        from renku.core.config import get_value
        from renku.domain_model.enums import ConfigFilter

        if not self._top.datadir:
            datadir = get_value("renku", DATA_DIR_CONFIG_KEY, config_filter=ConfigFilter.LOCAL_ONLY)
            self._top.datadir = datadir or DEFAULT_DATA_DIR

        return self._top.datadir

    @datadir.setter
    def datadir(self, value: str):
        """Set the current datadir."""
        self._top.datadir = value

    @property
    def dataset_images_path(self) -> Path:
        """Return a ``Path`` instance of Renku dataset metadata folder."""
        return self.path / RENKU_HOME / DATASET_IMAGES

    @property
    def docker_path(self):
        """Path to the Dockerfile."""
        return self.path / DOCKERFILE

    @property
    def empty(self) -> bool:
        """Return True if there is no pushed context."""
        return not bool(self._context_stack)

    @property
    def global_config_dir(self) -> str:
        """Return user's config directory."""
        import click

        return click.get_app_dir(APP_NAME, force_posix=True)

    @property
    def global_config_path(self) -> Path:
        """Renku global (user's) config path."""
        config = Path(self.global_config_dir)
        if not config.exists():
            config.mkdir(parents=True)

        return config / CONFIG_NAME

    @property
    def latest_agent(self) -> Optional[str]:
        """Returns latest agent version used in the repository."""
        try:
            return self.project.agent_version
        except ValueError as e:
            if "Project name not set" in str(e):
                return None
            raise

    @property
    def local_config_path(self) -> Path:
        """Renku local (project) config path."""
        return self.metadata_path / CONFIG_NAME

    @property
    def lock(self):
        """Create a Renku config lock."""
        from renku.core.util.contexts import Lock

        return Lock(filename=self.metadata_path.with_suffix(LOCK_SUFFIX), mode="exclusive")

    @property
    def metadata_path(self) -> Path:
        """Current project's metadata (RENKU_HOME) path."""
        return self.path / RENKU_HOME

    @property
    def path(self) -> Path:
        """Current project path."""
        return self._top.path

    @property
    def pointers_path(self) -> Path:
        """Return a ``Path`` instance of Renku pointer files folder."""
        path = self.path / RENKU_HOME / POINTERS
        path.mkdir(exist_ok=True)
        return path

    @property
    def project(self) -> "Project":
        """Return the Project instance."""
        if not self._top.project:
            try:
                self._top.project = self.database["project"]  # type: ignore
            except KeyError:
                raise ValueError(f"Project is not set in '{self.path}'")

        return self._top.project  # type: ignore

    @property
    def remote(self) -> "ProjectRemote":
        """Return host, owner and name of the remote if it exists."""
        repository = self.repository

        remote: Optional["Remote"]
        if repository.active_branch and repository.active_branch.remote_branch:
            remote = repository.active_branch.remote_branch.remote
        elif len(repository.remotes) == 1:
            remote = repository.remotes[0]
        else:
            remote = repository.remotes.get("origin")

        return ProjectRemote.from_remote(remote=remote)

    @property
    def repository(self) -> "Repository":
        """Return current context's repository."""
        if not self._top.repository:
            from renku.infrastructure.repository import Repository

            try:
                self._top.repository = Repository(project_properties.path)
            except errors.GitError as e:
                raise ValueError from e

        return self._top.repository

    @repository.setter
    def repository(self, value: "Repository"):
        """Set the current repository."""
        self._top.repository = value

    @property
    def template_checksums_path(self):
        """Return a ``Path`` instance to the template checksums file."""
        return self.metadata_path / TEMPLATE_CHECKSUMS

    @property
    def transaction_id(self) -> str:
        """Get a transaction id for the current client to be used for grouping git commits."""
        if not self._top.transaction_id:
            self._top.transaction_id = uuid.uuid4().hex

        return f"\n\nrenku-transaction: {self._top.transaction_id}"

    @property
    def _top(self) -> ProjectContext:
        """Return current context."""
        if self._context_stack:
            return self._context_stack[-1]

        raise errors.ConfigurationError("No project context was pushed")

    def clear(self) -> None:
        """Remove all contexts and reset the state without committing intermediate changes.

        NOTE: This method should be used only in tests.
        """
        while self._context_stack:
            if self._top.repository:
                self._top.repository.close()
            self._context_stack.pop()

        self.external_storage_requested = True

    def pop_context(self) -> ProjectContext:
        """Pop current project context from stack.

        Returns:
            Path: the popped project path.
        """
        if self._context_stack:
            if self._top.save_changes and self._top.database:
                self._top.database.commit()

            if self._top.repository:
                self._top.repository.close()

            return self._context_stack.pop()
        else:
            raise IndexError("No more context to pop.")

    def push_path(self, path: Union[Path, str], save_changes: bool = False) -> None:
        """Push a new project path to the stack.

        Arguments:
            path(Union[Path, str]): The path to push.
        """
        path = Path(path).resolve()
        self._context_stack.append(ProjectContext(path=path, save_changes=save_changes))

    def replace_path(self, path: Union[Path, str]):
        """Replace the current project path with a new one if they are different.

        Arguments:
            path(Union[Path, str]): The path to replace with.
        """
        path = Path(path).resolve()

        if not self._context_stack:
            self.push_path(path)
        elif self._top.path != path:
            self._context_stack[-1] = ProjectContext(path=path)

    def reset_project(self) -> None:
        """Discard cached project value."""
        if self._context_stack:
            self._top.project = None

    @contextlib.contextmanager
    def with_path(self, path: Union[Path, str], save_changes: bool = False) -> Generator[ProjectContext, None, None]:
        """Temporarily push a new project path to the stack.

        Arguments:
            path(Union[Path, str]): The path to push.
        """
        with self.with_rollback():
            self.push_path(path=path, save_changes=save_changes)
            yield self._top

    @contextlib.contextmanager
    def with_rollback(self) -> Generator[None, None, None]:
        """Rollback to the current state.

        NOTE: This won't work correctly if the current context is popped or swapped.
        """
        before_top = self._top if self._context_stack else None

        try:
            yield
        finally:
            could_rollback = False

            while self._context_stack:
                if self._top == before_top:
                    could_rollback = True
                    break

                self.pop_context()

            if not could_rollback and before_top is not None:
                raise errors.ConfigurationError(f"Cannot rollback to {before_top.path}.")


project_properties: ProjectProperties = ProjectProperties()


def has_graph_files() -> bool:
    """Return true if database exists."""
    return project_properties.database_path.exists() and any(
        f for f in project_properties.database_path.iterdir() if f != project_properties.database_path / "root"
    )


class ProjectRemote(NamedTuple):
    """Information about a project's remote."""

    name: Optional[str]
    owner: Optional[str]
    host: Optional[str]

    @classmethod
    def from_remote(cls, remote: Optional["Remote"]) -> "ProjectRemote":
        """Create an instance from a Repository remote."""
        from renku.domain_model.git import GitURL

        if not remote:
            return ProjectRemote(None, None, None)

        url = GitURL.parse(remote.url)

        # NOTE: Remove gitlab unless running on gitlab.com
        hostname_parts = url.hostname.split(".")
        if len(hostname_parts) > 2 and hostname_parts[0] == "gitlab":
            hostname_parts = hostname_parts[1:]

        return ProjectRemote(name=url.name, owner=url.owner, host=".".join(hostname_parts))

    def __bool__(self):
        return bool(self.name or self.owner or self.host)
