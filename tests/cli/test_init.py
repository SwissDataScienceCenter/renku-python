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
"""CLI tests for init command."""

import os
import shutil

from renku import cli
from renku._compat import Path


def test_init(isolated_runner):
    """Test project initialization."""
    runner = isolated_runner

    # 1. the directory should be automatically created
    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = runner.invoke(cli.cli, ['init', 'test-new-project'])
    assert 0 == result.exit_code
    assert new_project.exists()

    # 2. test project directory creation
    os.mkdir('test-project')
    result = runner.invoke(cli.cli, ['init', 'test-project'])
    assert 0 == result.exit_code
    assert os.stat(os.path.join('test-project', '.git'))
    assert os.stat(os.path.join('test-project', '.renku'))

    # 3. test project init from already existing renku repository
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert 0 != result.exit_code

    # 4. in case of init failure because of existing .git folder
    #    .renku directory should not exist
    assert not os.path.exists(os.path.join('test-project', '.renku'))

    result = runner.invoke(cli.cli, ['init', '--force'])
    assert 0 == result.exit_code
    assert os.stat(os.path.join('.git'))
    assert os.stat(os.path.join('.renku'))

    # 5. check git lfs init options
    os.chdir('../')
    shutil.rmtree('test-project')
    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init', '--no-external-storage'])
    with open('.git/config') as f:
        config = f.read()
    assert 'filter "lfs"' not in config

    result = runner.invoke(cli.cli, ['init', '-S'])
    with open('.git/config') as f:
        config = f.read()
    assert 'filter "lfs"' not in config

    result = runner.invoke(cli.cli, ['init', '--force'])
    with open('.git/config') as f:
        config = f.read()
    assert 'filter "lfs"' in config


def test_do_not_override_existing_files(isolated_runner):
    """Run init with existing files."""
    runner = isolated_runner

    dockerfile = Path('Dockerfile')
    dockerfile_content = 'FROM alpine'
    with dockerfile.open('w') as fp:
        fp.write(dockerfile_content)

    requirements = Path('requirements.txt')
    requirements_content = 'pandas'
    with requirements.open('w') as fp:
        fp.write(requirements_content)

    # The order of answers depends on CI_TEMPLATES.
    # from renku.cli.runner import CI_TEMPLATES
    result = runner.invoke(cli.cli, ['init'], input='y\nn\n')
    assert 1 == result.exit_code

    with dockerfile.open() as fp:
        assert dockerfile_content != fp.read().strip()

    with requirements.open() as fp:
        assert requirements_content == fp.read().strip()


def test_init_on_cloned_repo(isolated_runner, data_repository):
    """Run init on a cloned repository."""
    runner = isolated_runner
    pwd = os.getcwd()

    # Clone to the current directory
    data_repository.clone(pwd)

    # Only --force should work
    result = runner.invoke(cli.cli, ['init'])
    assert 0 != result.exit_code

    result = runner.invoke(
        cli.cli, ['init', '--force'], catch_exceptions=False
    )
    assert 0 == result.exit_code


def test_init_force_in_empty_dir(isolated_runner):
    """Run init --force in empty directory."""
    runner = isolated_runner

    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = runner.invoke(cli.cli, ['init', '--force', 'test-new-project'])
    assert 0 == result.exit_code
