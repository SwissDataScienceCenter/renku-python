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
"""CLI tests."""

from __future__ import absolute_import, print_function

import contextlib
import os
import subprocess
import sys
from pathlib import Path

import git
import pytest
import yaml

from renku import __version__
from renku.cli import cli
from renku.core.management.storage import StorageApiMixin
from renku.core.models.cwl.ascwl import CWLClass, ascwl
from renku.core.models.cwl.workflow import Workflow


def test_version(runner):
    """Test cli version."""
    result = runner.invoke(cli, ['--version'])
    assert __version__ in result.output.split('\n')


@pytest.mark.parametrize('arg', (('help', ), ('-h', ), ('--help', )))
def test_help(arg, runner):
    """Test cli help."""
    result = runner.invoke(cli, [arg])
    assert 0 == result.exit_code
    assert 'Show this message and exit.' in result.output


def test_config_path(runner):
    """Test config path."""
    result = runner.invoke(cli, ['--config-path'])
    output = result.output.split('\n')[0]
    assert 'renku.ini' in output


def test_show_context(runner):
    """Test context generation."""
    import json

    result = runner.invoke(cli, ['show', 'context', '--list'])
    contexts = [name for name in result.output.split('\n') if name]
    assert 0 == result.exit_code
    assert 1 < len(contexts)

    result = runner.invoke(cli, ['show', 'context'] + contexts)
    data = json.loads(result.output)
    assert 0 == result.exit_code
    assert len(contexts) == len(data)


_CMD_EXIT_2 = ['bash', '-c', 'exit 2']


@pytest.mark.parametrize(
    'cmd, exit_code', (
        (_CMD_EXIT_2, 1),
        (['--success-code', '1', '--no-output'] + _CMD_EXIT_2, 1),
        (['--success-code', '2', '--no-output'] + _CMD_EXIT_2, 0),
        (['--success-code', '0', '--no-output', 'echo', 'hola'], 0),
    )
)
def test_exit_code(cmd, exit_code, runner, project):
    """Test exit-code of run command."""
    result = runner.invoke(cli, ['run'] + cmd)
    assert exit_code == result.exit_code


def test_git_pre_commit_hook(runner, project, capsys):
    """Test detection of output edits."""
    result = runner.invoke(cli, ['githooks', 'install'])
    assert 0 == result.exit_code
    assert 'Hook already exists.' in result.output

    repo = git.Repo(project)
    cwd = Path(project)
    output = cwd / 'output.txt'

    result = runner.invoke(cli, ['run', 'touch', output.name])
    assert 0 == result.exit_code
    with output.open('w') as f:
        f.write('hello')

    repo.git.add('--all')
    with pytest.raises(git.HookExecutionError) as error:
        repo.index.commit('hello')
        assert output.name in error.stdout

    result = runner.invoke(cli, ['githooks', 'uninstall'])
    assert 0 == result.exit_code

    repo.index.commit('hello')


def test_workflow(runner, project):
    """Test workflow command."""
    result = runner.invoke(cli, ['run', 'touch', 'data.csv'])
    assert 0 == result.exit_code

    with open('counted.txt', 'w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.main(
                    args=('run', 'wc', 'data.csv'),
                    prog_name=runner.get_default_prog_name(cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(
        cli,
        ['workflow', 'create', 'counted.txt', '-o', 'workflow.cwl'],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    with open('workflow.cwl', 'r') as f:
        workflow = Workflow.from_cwl(yaml.safe_load(f))
        assert workflow.steps[0].run.startswith('.renku/workflow/')

    # Compare default log and log for a specific file.
    result_default = runner.invoke(cli, ['log'])
    result_arg = runner.invoke(cli, ['log', 'counted.txt'])

    assert 0 == result_default.exit_code
    assert 0 == result_arg.exit_code
    assert result_default.output == result_arg.output


def test_streams(runner, project, capsys):
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
                        cli.main(
                            args=('run', 'cut', '-d,', '-f', '2', '-s'),
                            prog_name=runner.get_default_prog_name(cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with open('result.txt', 'r') as f:
        assert f.read().strip() == 'second'

    result = runner.invoke(cli, ['workflow', 'create', 'result.txt'])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['status'])
    assert 0 == result.exit_code

    # Check that source.txt is not shown in outputs.
    result = runner.invoke(cli, ['show', 'outputs', 'source.txt'])
    assert 1 == result.exit_code

    result = runner.invoke(cli, ['show', 'outputs'])
    assert 0 == result.exit_code
    assert {
        'result.txt',
    } == set(result.output.strip().split('\n'))

    # Check that source.txt is shown in inputs.
    result = runner.invoke(cli, ['show', 'inputs'])
    assert 0 == result.exit_code
    assert {
        'source.txt',
    } == set(result.output.strip().split('\n'))

    with open('source.txt', 'w') as source:
        source.write('first,second,third,fourth')

    repo.git.add('--all')
    repo.index.commit('Changed source.txt')

    result = runner.invoke(cli, ['status'])
    assert 1 == result.exit_code
    assert 'source.txt' in result.output


def test_streams_cleanup(runner, project, run):
    """Test cleanup of standard streams."""
    source = Path(project) / 'source.txt'
    stdout = Path(project) / 'result.txt'

    with source.open('w') as fp:
        fp.write('first,second,third')

    # File outside the Git index should be deleted.

    with source.open('r') as fp:
        assert fp.read() == 'first,second,third'

    assert not stdout.exists()

    result = runner.invoke(cli, ['status'])

    # Dirty repository check.
    assert 1 == result.exit_code

    # File from the Git index should be restored.
    repo = git.Repo(project)
    with stdout.open('w') as fp:
        fp.write('1')

    repo.index.add(['result.txt'])

    with stdout.open('r') as fp:
        assert fp.read() == '1'


def test_streams_and_args_names(runner, project, capsys):
    """Test streams and conflicting argument names."""
    with capsys.disabled():
        with open('lalala', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=('run', 'echo', 'lalala'), )
                except SystemExit as e:
                    assert e.code in {None, 0}
            finally:
                sys.stdout = old_stdout

    with open('lalala', 'r') as f:
        assert f.read().strip() == 'lalala'

    result = runner.invoke(cli, ['status'], catch_exceptions=False)
    assert 0 == result.exit_code


def test_show_inputs(tmpdir_factory, project, runner, run):
    """Test show inputs with submodules."""
    second_project = Path(str(tmpdir_factory.mktemp('second_project')))

    assert 0 == run(args=('init', str(second_project)))

    woop = second_project / 'woop'
    with woop.open('w') as fp:
        fp.write('woop')

    second_repo = git.Repo(str(second_project))
    second_repo.git.add('--all')
    second_repo.index.commit('Added woop file')

    assert 0 == run(args=('dataset', 'create', 'foo'))
    assert 0 == run(args=('dataset', 'add', 'foo', str(woop)))

    imported_woop = Path(project) / 'data' / 'foo' / woop.name
    assert imported_woop.exists()

    woop_wc = Path(project) / 'woop.wc'
    assert 0 == run(args=('run', 'wc'), stdin=imported_woop, stdout=woop_wc)

    result = runner.invoke(cli, ['show', 'inputs'], catch_exceptions=False)
    assert {str(imported_woop.resolve().relative_to(Path(project).resolve()))
            } == set(result.output.strip().split('\n'))


def test_configuration_of_no_external_storage(isolated_runner, monkeypatch):
    """Test the LFS requirement for renku run with disabled storage."""
    runner = isolated_runner

    os.mkdir('test-project')
    os.chdir('test-project')

    result = runner.invoke(cli, ['--no-external-storage', 'init'])
    assert 0 == result.exit_code
    # Pretend that git-lfs is not installed.
    with monkeypatch.context() as monkey:
        monkey.setattr(StorageApiMixin, 'storage_installed', False)
        # Missing --no-external-storage flag.
        result = runner.invoke(cli, ['run', 'touch', 'output'])
        assert 'is not configured' in result.output
        assert 1 == result.exit_code

        # Since repo is not using external storage.
        result = runner.invoke(
            cli, ['--no-external-storage', 'run', 'touch', 'output']
        )
        assert 0 == result.exit_code

    subprocess.call(['git', 'clean', '-df'])
    result = runner.invoke(
        cli, ['--no-external-storage', 'run', 'touch', 'output']
    )
    # Needs to result in error since output file
    # is now considered an input file (check run.py doc).
    assert 1 == result.exit_code


def test_configuration_of_external_storage(isolated_runner, monkeypatch):
    """Test the LFS requirement for renku run."""
    runner = isolated_runner

    result = runner.invoke(cli, ['--external-storage', 'init'])
    assert 0 == result.exit_code
    # Pretend that git-lfs is not installed.
    with monkeypatch.context() as monkey:
        monkey.setattr(StorageApiMixin, 'storage_installed', False)
        # Repo is using external storage but it's not installed.
        result = runner.invoke(cli, ['run', 'touch', 'output'])
        assert 'is not configured' in result.output
        assert 1 == result.exit_code

    # Clean repo and check external storage.
    subprocess.call(['git', 'clean', '-df'])
    result = runner.invoke(cli, ['run', 'touch', 'output2'])
    assert 0 == result.exit_code


def test_file_tracking(isolated_runner):
    """Test .gitattribute handling on renku run."""
    runner = isolated_runner

    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli, ['init'])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['run', 'touch', 'output'])
    assert 0 == result.exit_code
    assert 'output' in Path('.gitattributes').read_text()


@pytest.mark.xfail
def test_status_with_submodules(isolated_runner, monkeypatch):
    """Test status calculation with submodules."""
    runner = isolated_runner

    os.mkdir('foo')
    os.mkdir('bar')

    with open('woop', 'w') as f:
        f.write('woop')

    os.chdir('foo')
    result = runner.invoke(
        cli, ['init', '--no-external-storage'], catch_exceptions=False
    )
    assert 0 == result.exit_code

    os.chdir('../bar')
    result = runner.invoke(
        cli, ['init', '--no-external-storage'], catch_exceptions=False
    )
    assert 0 == result.exit_code

    os.chdir('../foo')
    with monkeypatch.context() as monkey:
        monkey.setattr(StorageApiMixin, 'storage_installed', False)

        result = runner.invoke(
            cli, ['dataset', 'add', 'f', '../woop'], catch_exceptions=False
        )

        assert 1 == result.exit_code
        subprocess.call(['git', 'clean', '-dff'])

    result = runner.invoke(
        cli, ['-S', 'dataset', 'add', 'f', '../woop'], catch_exceptions=False
    )
    assert 0 == result.exit_code

    os.chdir('../bar')
    result = runner.invoke(
        cli, ['-S', 'dataset', 'add', 'b', '../foo/data/f/woop'],
        catch_exceptions=False
    )
    assert 0 == result.exit_code

    # Produce a derived data from the imported data.
    with open('woop.wc', 'w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.main(
                    args=('-S', 'run', 'wc', 'data/b/woop'),
                    prog_name=runner.get_default_prog_name(cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(cli, ['status'], catch_exceptions=False)
    assert 0 == result.exit_code

    # Modify the source data.
    os.chdir('../foo')
    with open('data/f/woop', 'w') as f:
        f.write('woop2')

    subprocess.call(['git', 'commit', '-am', 'commiting changes to woop'])

    os.chdir('../bar')
    subprocess.call(['git', 'submodule', 'update', '--rebase', '--remote'])
    subprocess.call(['git', 'commit', '-am', 'update submodule'])

    result = runner.invoke(cli, ['status'], catch_exceptions=False)
    assert 0 != result.exit_code

    # Test relative log output
    cmd = ['--path', '../foo', 'log']
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert '../foo/data/f/woop' in result.output
    assert 0 == result.exit_code


def test_status_consistency(runner, client, project):
    """Test if the renku status output is consistent when running the
    command from directories other than the repository root."""

    os.mkdir('somedirectory')
    with open('somedirectory/woop', 'w') as fp:
        fp.write('woop')

    client.repo.index.add(['somedirectory/woop'])
    client.repo.index.commit('add woop')

    result = runner.invoke(
        cli, ['run', 'cp', 'somedirectory/woop', 'somedirectory/meeh']
    )
    assert 0 == result.exit_code

    with open('somedirectory/woop', 'w') as fp:
        fp.write('weep')

    client.repo.index.add(['somedirectory/woop'])
    client.repo.index.commit('fix woop')

    base_result = runner.invoke(cli, ['status'])
    os.chdir('somedirectory')
    comp_result = runner.invoke(cli, ['status'])
    assert base_result.output.replace(
        'somedirectory/', ''
    ) == comp_result.output


def test_unchanged_output(runner, project):
    """Test detection of unchanged output."""
    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code

    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert 1 == result.exit_code


def test_unchanged_stdout(runner, project, capsys):
    """Test detection of unchanged stdout."""
    with capsys.disabled():
        with open('output.txt', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=('run', 'echo', '1'), )
                except SystemExit as e:
                    assert e.code in {None, 0}
            finally:
                sys.stdout = old_stdout

    with capsys.disabled():
        with open('output.txt', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=('run', 'echo', '1'), )
                except SystemExit as e:
                    # The stdout has not been modified!
                    assert e.code in {None, 1}
            finally:
                sys.stdout = old_stdout


def test_modified_output(runner, project, run):
    """Test detection of changed file as output."""
    cwd = Path(project)
    source = cwd / 'source.txt'
    data = cwd / 'data' / 'results'
    data.mkdir(parents=True)
    output = data / 'result.txt'

    repo = git.Repo(project)
    cmd = ['run', 'cp', '-r', str(source), str(output)]

    def update_source(data):
        """Update source.txt."""
        with source.open('w') as fp:
            fp.write(data)

        repo.git.add('--all')
        repo.index.commit('Updated source.txt')

    update_source('1')

    # The output file does not exist.
    assert not output.exists()

    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    # The output file is copied from the source.
    with output.open('r') as f:
        assert f.read().strip() == '1'

    update_source('2')

    # The input file has been updated and output is recreated.
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    with output.open('r') as f:
        assert f.read().strip() == '2'

    update_source('3')

    # The input has been modifed and we check that the previous
    # run command correctly recognized output.txt.
    assert 0 == run()

    with output.open('r') as f:
        assert f.read().strip() == '3'


def test_modified_tool(runner, project, run):
    """Test detection of modified tool."""
    from renku.core.management import LocalClient

    client = LocalClient(project)
    repo = client.repo
    greeting = client.path / 'greeting.txt'

    assert 0 == run(args=('run', 'echo', 'hello'), stdout=greeting)

    cmd = ['status']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    # There should be only one command line tool.
    tools = list(client.workflow_path.glob('*_echo.cwl'))
    assert 1 == len(tools)

    tool_path = tools[0]
    with tool_path.open('r') as f:
        command_line_tool = CWLClass.from_cwl(yaml.safe_load(f))

    # Simulate a manual edit.
    command_line_tool.inputs[0].default = 'ahoj'
    command_line_tool.stdout = 'pozdrav.txt'

    with tool_path.open('w') as f:
        yaml.dump(
            ascwl(
                command_line_tool,
                filter=lambda _, x: x is not None,
                basedir=client.workflow_path,
            ),
            stream=f,
            default_flow_style=False
        )

    repo.git.add('--all')
    repo.index.commit('Modified tool', skip_hooks=True)

    assert 0 == run()

    output = client.path / 'pozdrav.txt'
    assert output.exists()
    with output.open('r') as f:
        assert 'ahoj\n' == f.read()

    cmd = ['status']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code


def test_siblings(runner, project):
    """Test detection of siblings."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    for sibling in siblings:
        cmd = ['show', 'siblings', sibling]
        result = runner.invoke(cli, cmd)
        assert 0 == result.exit_code

        output = {
            name.strip()
            for name in result.output.split('\n') if name.strip()
        }
        assert output == siblings, 'Checked {0}'.format(sibling)


def test_orphan(runner, project):
    """Test detection of an orphan."""
    cwd = Path(project)
    orphan = cwd / 'orphan.txt'

    cmd = ['run', 'touch', orphan.name]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    cmd = ['show', 'siblings', 'orphan.txt']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code
    assert 'orphan.txt\n' == result.output


def test_only_child(runner, project):
    """Test detection of an only child."""
    cmd = ['run', 'touch', 'only_child']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    cmd = ['show', 'siblings', 'only_child']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code
    assert 'only_child\n' == result.output


def test_outputs(runner, project):
    """Test detection of outputs."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['show', 'outputs'])
    assert 0 == result.exit_code
    assert siblings == set(result.output.strip().split('\n'))


def test_moved_file(runner, project):
    """Test that moved files are displayed correctly."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    result = runner.invoke(cli, ['log'])
    assert 0 == result.exit_code
    assert input_.name in result.output

    repo.git.mv(input_.name, 'renamed.txt')
    repo.index.commit('Renamed input')

    result = runner.invoke(cli, ['log'])
    assert 0 == result.exit_code
    assert input_.name not in result.output
    assert 'renamed.txt' in result.output


def test_deleted_input(runner, project, capsys):
    """Test deleted input."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    cmd = ['run', 'mv', input_.name, 'input.mv']
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code
    assert not input_.exists()
    assert Path('input.mv').exists()


def test_input_directory(runner, project, run):
    """Test detection of input directory."""
    repo = git.Repo(project)
    cwd = Path(project)
    output = cwd / 'output.txt'
    inputs = cwd / 'inputs'
    inputs.mkdir(parents=True)

    gitkeep = inputs / '.gitkeep'
    gitkeep.touch()
    repo.git.add('--all')
    repo.index.commit('Empty inputs directory')

    assert 0 == run(args=('run', 'ls', str(inputs)), stdout=output)
    with output.open('r') as fp:
        assert '' == fp.read().strip()

    (inputs / 'first').touch()

    repo.git.add('--all')
    repo.index.commit('Created inputs')

    assert 0 == run(args=('update', output.name))

    with output.open('r') as fp:
        assert 'first\n' == fp.read()

    (inputs / 'second').touch()
    repo.git.add('--all')
    repo.index.commit('Added second input')

    assert 0 == run(args=('update', output.name))
    with output.open('r') as fp:
        assert 'first\nsecond\n' == fp.read()

    result = runner.invoke(cli, ['show', 'inputs'])
    assert set(
        str(p.relative_to(cwd))
        for p in inputs.rglob('*') if p.name != '.gitkeep'
    ) == set(result.output.strip().split('\n'))


def test_config_manager_creation(client):
    """Check creation of configuration file."""
    path_ = client.config_path
    assert str(path_).endswith('renku.ini')
    config = client.load_config()
    client.store_config(config)
    assert path_.exists()


def test_config_manager_set_value(client):
    """Check writing to configuration."""
    client.set_value('zenodo', 'access_token', 'my-secret')

    config = client.load_config()
    assert config.get('zenodo', 'access_token') == 'my-secret'

    config = client.remove_value('zenodo', 'access_token')
    assert 'zenodo' not in config.sections()


def test_config_load_get_value(client):
    """Check reading from configuration."""
    client.set_value('zenodo', 'access_token', 'my-secret')
    secret = client.get_value('zenodo', 'access_token')
    assert secret == 'my-secret'

    secret = client.get_value('zenodo2', 'access_token')
    assert secret is None

    secret = client.get_value('zenodo', 'not-secret')
    assert secret is None

    config = client.remove_value('zenodo', 'access_token')
    assert 'zenodo' not in config.sections()


def test_config_manager_cli(client, runner, project, config_dir):
    """Check config command for global cfg."""
    result = runner.invoke(
        cli, [
            'config', 'registry', 'http://demo:demo@global.example.com',
            '--global'
        ]
    )
    assert 'http://demo:demo@global.example.com\n' == result.output
    assert 0 == result.exit_code

    value = client.get_value('renku', 'registry')
    assert 'http://demo:demo@global.example.com' == value
