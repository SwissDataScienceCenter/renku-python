# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
"""CLI tests."""

from __future__ import absolute_import, print_function

import contextlib
import os
import shutil
import sys
from subprocess import call

import git
import pytest
import yaml

from renku import __version__, cli
from renku.models.cwl.workflow import Workflow


def test_version(base_runner):
    """Test cli version."""
    result = base_runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


@pytest.mark.parametrize('arg', (('help', ), ('-h', ), ('--help', )))
def test_help(arg, base_runner):
    """Test cli help."""
    result = base_runner.invoke(cli.cli, [arg])
    assert result.exit_code == 0
    assert 'Show this message and exit.' in result.output


def test_config_path(instance_path, base_runner):
    """Test config path."""
    result = base_runner.invoke(cli.cli, ['--config-path'])
    output = result.output.split('\n')[0]
    assert 'config.yml' in output
    assert instance_path in output


def test_init(base_runner):
    """Test project initialization."""
    runner = base_runner

    # 1. the directory must exist
    result = runner.invoke(cli.cli, ['init', 'test-project'])
    assert result.exit_code == 2

    # 2. test project directory creation
    os.mkdir('test-project')
    result = runner.invoke(cli.cli, ['init', 'test-project'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('test-project', '.git'))
    assert os.stat(os.path.join('test-project', '.renku'))

    # 3. test project init from already existing renku repository
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert result.exit_code != 0

    # 4. in case of init failure because of existing .git folder
    #    .renku directory should not exist
    assert not os.path.exists(os.path.join('test-project', '.renku'))

    result = runner.invoke(cli.cli, ['init', '--force'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('.git'))
    assert os.stat(os.path.join('.renku'))

    # 4. check git lfs init options
    os.chdir('../')
    shutil.rmtree('test-project')
    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init', '--no-external-storage'])
    with open('.git/config') as f:
        config = f.read()
    assert 'filter "lfs"' not in config

    result = runner.invoke(cli.cli, ['init', '--force'])
    with open('.git/config') as f:
        config = f.read()
    assert 'filter "lfs"' in config


def test_workon(runner):
    """Test switching branches."""
    # Create first issue
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['deactivate'])
    assert result.exit_code == 0

    # Enter existing
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['deactivate'])
    assert result.exit_code == 0


def test_run_simple(runner):
    """Test tracking of run command."""
    cmd = ['echo', 'test']
    result = runner.invoke(cli.cli, ['run', '--no-output'] + cmd)
    assert result.exit_code == 0


def test_workflow(runner):
    """Test workflow command."""
    result = runner.invoke(cli.cli, ['run', 'touch', 'data.csv'])
    assert result.exit_code == 0

    with open('counted.txt', 'w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.cli.main(
                    args=('run', 'wc', 'data.csv'),
                    prog_name=runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(
        cli.cli, ['workflow', 'create', 'counted.txt', '-o', 'workflow.cwl']
    )
    assert result.exit_code == 0

    with open('workflow.cwl', 'r') as f:
        workflow = Workflow.from_cwl(yaml.load(f))
        assert workflow.steps[0].run.startswith('.renku/workflow/')


def test_streams(runner, capsys):
    """Test redirection of std streams."""
    repo = git.Repo('.')

    with open('source.txt', 'w') as source:
        source.write('first,second,third')

    repo.git.add('--all')
    repo.index.commit('Added source.txt')

    with capsys.disabled():
        with open('source.txt', 'rb') as stdin:
            with open('result.txt', 'wb') as stdout:
                try:
                    old_stdin, old_stdout = sys.stdin, sys.stdout
                    sys.stdin, sys.stdout = stdin, stdout
                    try:
                        cli.cli.main(
                            args=('run', 'cut', '-d,', '-f', '2', '-s'),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with open('result.txt', 'r') as f:
        assert f.read().strip() == 'second'

    result = runner.invoke(cli.cli, ['workflow', 'create', 'result.txt'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 0

    with open('source.txt', 'w') as source:
        source.write('first,second,third,fourth')

    repo.git.add('--all')
    repo.index.commit('Changed source.txt')

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1
    assert 'source.txt' in result.output


def test_datasets(data_file, data_repository, runner):
    """Test importing data into a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/metadata.yml')

    # add data
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'dataset',
                  str(data_file)]
    )
    assert result.exit_code == 0
    assert os.stat(
        os.path.join('data', 'dataset', os.path.basename(data_file))
    )

    # add data from a git repo via http
    result = runner.invoke(
        cli.cli, [
            'dataset', 'add', 'dataset', '--target', 'README.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ]
    )
    assert result.exit_code == 0
    assert os.stat('data/dataset/renku-python/README.rst')

    # add data from local git repo
    result = runner.invoke(
        cli.cli, [
            'dataset', 'add', 'dataset', '-t', 'file', '-t', 'file2',
            os.path.dirname(data_repository.git_dir)
        ]
    )
    assert result.exit_code == 0


def test_file_tracking(base_runner):
    """Test .gitattribute handling on renku run."""
    runner = base_runner

    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['run', 'touch', 'output'])
    assert result.exit_code == 0

    with open('.gitattributes') as f:
        gitattributes = f.read()
    assert 'output' in gitattributes


def test_status_with_submodules(base_runner):
    """Test status calculation with submodules."""
    os.mkdir('foo')
    os.mkdir('bar')

    with open('woop', 'w') as f:
        f.write('woop')

    os.chdir('foo')
    result = base_runner.invoke(
        cli.cli, ['init', '-S'], catch_exceptions=False
    )
    assert result.exit_code == 0

    os.chdir('../bar')
    result = base_runner.invoke(
        cli.cli, ['init', '-S'], catch_exceptions=False
    )
    assert result.exit_code == 0

    os.chdir('../foo')
    result = base_runner.invoke(
        cli.cli, ['dataset', 'add', 'f', '../woop'], catch_exceptions=False
    )
    assert result.exit_code == 0

    os.chdir('../bar')
    result = base_runner.invoke(
        cli.cli, ['dataset', 'add', 'b', '../foo/data/f/woop'],
        catch_exceptions=False
    )
    assert result.exit_code == 0

    # Produce a derived data from the imported data.
    with open('woop.wc', 'w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.cli.main(
                    args=('run', 'wc', 'data/b/foo/data/f/woop'),
                    prog_name=base_runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = base_runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert result.exit_code == 0

    # Modify the source data.
    os.chdir('../foo')
    with open('data/f/woop', 'w') as f:
        f.write('woop2')

    call(['git', 'commit', '-am', 'commiting changes to woop'])

    os.chdir('../bar')
    call(['git', 'submodule', 'update', '--rebase', '--remote'])
    call(['git', 'commit', '-am', 'update submodule'])

    result = base_runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert result.exit_code != 0
