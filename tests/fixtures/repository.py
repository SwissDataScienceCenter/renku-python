# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku fixtures for repository management."""

import contextlib
import os
import secrets
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator

import pytest

from renku.core.config import set_value
from renku.core.constant import DATABASE_PATH, POINTERS, RENKU_HOME
from renku.core.util import communication
from renku.core.util.contexts import chdir
from renku.domain_model.project_context import ProjectContext, project_context
from renku.infrastructure.repository import Repository
from renku.ui.cli.init import init
from tests.utils import format_result_exception, modified_environ


@dataclass
class RenkuProject:
    """A Renku project for use in tests to access project properties."""

    path: Path
    repository: Repository
    metadata_path: Path = field(init=False)
    database_path: Path = field(init=False)
    pointers_path: Path = field(init=False)

    def __post_init__(self):
        self.metadata_path = self.path / RENKU_HOME
        self.database_path = self.path / RENKU_HOME / DATABASE_PATH
        self.pointers_path = self.path / RENKU_HOME / POINTERS


@contextlib.contextmanager
def isolated_filesystem(path: Path, name: str = None, delete: bool = True):
    """Click CliRunner ``isolated_filesystem`` but xdist compatible."""
    name = name or secrets.token_hex(8)

    base_path = path / name
    base_path = base_path.resolve()
    base_path.mkdir(parents=True, exist_ok=True)

    with chdir(base_path):
        try:
            yield base_path
        finally:
            if delete:
                try:
                    shutil.rmtree(base_path)
                except OSError:  # noqa: B014
                    pass


@pytest.fixture
def fake_home(tmp_path, monkeypatch) -> Generator[Path, None, None]:
    """Yield a fake home directory with global config values."""
    home = tmp_path / "user_home"
    home.mkdir(parents=True, exist_ok=True)
    home_str = home.as_posix()

    with modified_environ(HOME=home_str, XDG_CONFIG_HOME=home_str), monkeypatch.context() as context:
        context.setattr(ProjectContext, "global_config_dir", os.path.join(home, ".renku"))

        # NOTE: fake user home directory
        with Repository.get_global_configuration(writable=True) as global_config:
            global_config.set_value("user", "name", "Renku Bot")
            global_config.set_value("user", "email", "renku@datascience.ch")
            global_config.set_value("pull", "rebase", "false")

        set_value(section="renku", key="show_lfs_message", value="False", global_only=True)

        yield home


@pytest.fixture
def project(fake_home) -> Generator[RenkuProject, None, None]:
    """A Renku test project."""
    from tests.fixtures.runners import RenkuRunner

    project_context.clear()

    with isolated_filesystem(fake_home.parent, delete=True) as project_path:
        with project_context.with_path(project_path):
            communication.disable()
            result = RenkuRunner().invoke(init, [".", "--template-id", "python-minimal"], "\n", catch_exceptions=False)
            communication.enable()
            assert 0 == result.exit_code, format_result_exception(result)

            repository = Repository(project_path, search_parent_directories=True)
            project_context.repository = repository

            yield RenkuProject(path=repository.path, repository=repository)
