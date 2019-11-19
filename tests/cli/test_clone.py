# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Renku clone tests."""

from pathlib import Path

from renku.cli import cli
from renku.core.management.storage import StorageApiMixin


def test_renku_clone(runner, remote_project, directory_tree, monkeypatch):
    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(
            cli, ['clone', directory_tree.strpath, project_path]
        )
        assert 0 == result.exit_code
        assert (Path(project_path) / 'dir2' / 'file2').exists()

        # Check Git hooks are installed
        result = runner.invoke(cli, ['githooks', 'install'])
        assert 0 == result.exit_code
        assert 'Hook already exists.' in result.output

        # Check Git LFS is enabled
        with monkeypatch.context() as monkey:
            # Pretend that git-lfs is not installed.
            monkey.setattr(StorageApiMixin, 'storage_installed', False)
            # Repo is using external storage but it's not installed.
            result = runner.invoke(cli, ['run', 'touch', 'output'])
            assert 'is not configured' in result.output
            assert 1 == result.exit_code
