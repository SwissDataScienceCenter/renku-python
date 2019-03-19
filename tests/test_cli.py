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
import shutil
import subprocess
import sys

import git
import pytest
import yaml

from renku import __version__, cli
from renku._compat import Path
from renku.models.cwl import CWLClass, ascwl
from renku.models.cwl.workflow import Workflow


def test_version(runner):
    """Test cli version."""
    result = runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


@pytest.mark.parametrize('arg', (('help', ), ('-h', ), ('--help', )))
def test_help(arg, runner):
    """Test cli help."""
    result = runner.invoke(cli.cli, [arg])
    assert 0 == result.exit_code
    assert 'Show this message and exit.' in result.output


def test_config_path(runner):
    """Test config path."""
    from renku.cli._config import RENKU_HOME

    result = runner.invoke(cli.cli, ['--config-path'])
    output = result.output.split('\n')[0]
    assert 'config.yml' in output
    assert RENKU_HOME in output


def test_show_context(runner):
    """Test context generation."""
    import json

    result = runner.invoke(cli.cli, ['show', 'context', '--list'])
    contexts = [name for name in result.output.split('\n') if name]
    assert 0 == result.exit_code
    assert 1 < len(contexts)

    result = runner.invoke(cli.cli, ['show', 'context'] + contexts)
    data = json.loads(result.output)
    assert 0 == result.exit_code
    assert len(contexts) == len(data)


def test_workon(runner, project):
    """Test switching branches."""
    # Create first issue
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['deactivate'])
    assert 0 == result.exit_code

    # Enter existing
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['deactivate'])
    assert 0 == result.exit_code


def test_run_simple(runner, project):
    """Test tracking of run command."""
    cmd = ['echo', 'test']
    result = runner.invoke(cli.cli, ['run', '--no-output'] + cmd)
    assert 0 == result.exit_code

    # There are no output files.
    result = runner.invoke(cli.cli, ['log'])
    assert 1 == len(result.output.strip().split('\n'))

    # Display tools with no outputs.
    result = runner.invoke(cli.cli, ['log', '--no-output'])
    assert '.renku/workflow/' in result.output


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
    result = runner.invoke(cli.cli, ['run'] + cmd)
    assert exit_code == result.exit_code


def test_git_pre_commit_hook(runner, project, capsys):
    """Test detection of output edits."""
    result = runner.invoke(cli.cli, ['githooks', 'install'])
    assert 0 == result.exit_code
    assert 'Hook already exists.' in result.output

    repo = git.Repo(project)
    cwd = Path(project)
    output = cwd / 'output.txt'

    result = runner.invoke(cli.cli, ['run', 'touch', output.name])
    assert 0 == result.exit_code

    with output.open('w') as f:
        f.write('hello')

    repo.git.add('--all')
    with pytest.raises(git.HookExecutionError) as error:
        repo.index.commit('hello')
        assert output.name in error.stdout

    result = runner.invoke(cli.cli, ['githooks', 'uninstall'])
    assert 0 == result.exit_code

    repo.index.commit('hello')


def test_workflow(runner, project):
    """Test workflow command."""
    result = runner.invoke(cli.cli, ['run', 'touch', 'data.csv'])
    assert 0 == result.exit_code

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
        cli.cli,
        ['workflow', 'create', 'counted.txt', '-o', 'workflow.cwl'],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    with open('workflow.cwl', 'r') as f:
        workflow = Workflow.from_cwl(yaml.safe_load(f))
        assert workflow.steps[0].run.startswith('.renku/workflow/')

    # Compare default log and log for a specific file.
    result_default = runner.invoke(cli.cli, ['log'])
    result_arg = runner.invoke(cli.cli, ['log', 'counted.txt'])

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
                        cli.cli.main(
                            args=('run', 'cut', '-d,', '-f', '2', '-s'),
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with open('result.txt', 'r') as f:
        assert f.read().strip() == 'second'

    result = runner.invoke(cli.cli, ['workflow', 'create', 'result.txt'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['status'])
    assert 0 == result.exit_code

    # Check that source.txt is not shown in outputs.
    result = runner.invoke(cli.cli, ['show', 'outputs', 'source.txt'])
    assert 1 == result.exit_code

    result = runner.invoke(cli.cli, ['show', 'outputs'])
    assert 0 == result.exit_code
    assert {
        'result.txt',
    } == set(result.output.strip().split('\n'))

    # Check that source.txt is shown in inputs.
    result = runner.invoke(cli.cli, ['show', 'inputs'])
    assert 0 == result.exit_code
    assert {
        'source.txt',
    } == set(result.output.strip().split('\n'))

    with open('source.txt', 'w') as source:
        source.write('first,second,third,fourth')

    repo.git.add('--all')
    repo.index.commit('Changed source.txt')

    result = runner.invoke(cli.cli, ['status'])
    assert 1 == result.exit_code
    assert 'source.txt' in result.output


def test_streams_cleanup(runner, project, run):
    """Test cleanup of standard streams."""
    source = Path(project) / 'source.txt'
    stdout = Path(project) / 'result.txt'

    with source.open('w') as fp:
        fp.write('first,second,third')

    # File outside the Git index should be deleted.
    assert 1 == run(
        args=('run', 'cat', source.name),
        stdout=stdout,
    ), 'The repo must be dirty.'

    with source.open('r') as fp:
        assert fp.read() == 'first,second,third'

    assert not stdout.exists()

    result = runner.invoke(cli.cli, ['status'])
    assert 1 == result.exit_code

    # File from the Git index should be restored.
    repo = git.Repo(project)
    with stdout.open('w') as fp:
        fp.write('1')

    repo.index.add(['result.txt'])

    assert 1 == run(
        args=('run', 'cat', 'source.txt'),
        stdout=stdout,
    ), 'The repo must be dirty.'

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
                    cli.cli.main(args=('run', 'echo', 'lalala'), )
                except SystemExit as e:
                    assert e.code in {None, 0}
            finally:
                sys.stdout = old_stdout

    with open('lalala', 'r') as f:
        assert f.read().strip() == 'lalala'

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert 0 == result.exit_code


def test_submodule_init(tmpdir_factory, runner, run, project):
    """Test initializing submodules."""

    src_project = Path(str(tmpdir_factory.mktemp('src_project')))

    assert 0 == run(args=('init', str(src_project)))

    woop = src_project / 'woop'
    with woop.open('w') as fp:
        fp.write('woop')

    repo = git.Repo(str(src_project))
    repo.git.add('--all')
    repo.index.commit('Added woop file')

    assert 0 == run(args=('dataset', 'create', 'foo'))
    assert 0 == run(args=('dataset', 'add', 'foo', str(woop)))

    imported_woop = Path(project) / 'data' / 'foo' / woop.name
    assert imported_woop.exists()

    dst_project = Path(str(tmpdir_factory.mktemp('dst_project')))
    subprocess.call(['git', 'clone', project, str(dst_project)])
    subprocess.call(['git', 'lfs', 'install', '--local'], cwd=str(dst_project))
    dst_woop = Path(dst_project) / 'data' / 'foo' / 'woop'
    assert not dst_woop.exists()
    result = runner.invoke(
        cli.cli, [
            '--path',
            str(dst_project), 'run', '--no-output', 'wc',
            str(dst_woop.absolute())
        ],
        catch_exceptions=False
    )
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

    result = runner.invoke(cli.cli, ['show', 'inputs'], catch_exceptions=False)
    assert {str(imported_woop.resolve().relative_to(Path(project).resolve()))
            } == set(result.output.strip().split('\n'))


def test_configuration_of_external_storage(isolated_runner, monkeypatch):
    """Test the LFS requirement for renku run."""
    runner = isolated_runner

    os.mkdir('test-project')
    os.chdir('test-project')

    result = runner.invoke(cli.cli, ['-S', 'init'])
    assert 0 == result.exit_code

    with monkeypatch.context() as m:
        from renku.api.storage import StorageApiMixin
        m.setattr(StorageApiMixin, 'external_storage_installed', False)

        result = runner.invoke(cli.cli, ['run', 'touch', 'output'])
        assert 1 == result.exit_code
        subprocess.call(['git', 'clean', '-df'])

    result = runner.invoke(cli.cli, ['-S', 'run', 'touch', 'output'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['init', '--force'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['run', 'touch', 'output2'])
    assert 0 == result.exit_code


def test_file_tracking(isolated_runner):
    """Test .gitattribute handling on renku run."""
    runner = isolated_runner

    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['run', 'touch', 'output'])
    assert 0 == result.exit_code

    with open('.gitattributes') as f:
        gitattributes = f.read()
    assert 'output' in gitattributes


def test_status_with_old_repository(isolated_runner, old_project):
    """Test status on all old repositories created by old version of renku."""
    runner = isolated_runner

    result = runner.invoke(cli.cli, ['status'])
    assert 0 == result.exit_code

    output = result.output.split('\n')
    assert output.pop(0) == 'On branch master'
    assert output.pop(0) == 'All files were generated from the latest inputs.'


def test_update_with_old_repository(isolated_runner, old_project):
    """Test update on all old repositories created by old version of renku."""
    runner = isolated_runner

    result = runner.invoke(cli.cli, ['update'])
    assert 0 == result.exit_code

    output = result.output.split('\n')
    assert output.pop(0) == 'All files were generated from the latest inputs.'


def test_status_with_submodules(isolated_runner, monkeypatch):
    """Test status calculation with submodules."""
    runner = isolated_runner

    os.mkdir('foo')
    os.mkdir('bar')

    with open('woop', 'w') as f:
        f.write('woop')

    os.chdir('foo')
    result = runner.invoke(cli.cli, ['init', '-S'], catch_exceptions=False)
    assert 0 == result.exit_code

    os.chdir('../bar')
    result = runner.invoke(cli.cli, ['init', '-S'], catch_exceptions=False)
    assert 0 == result.exit_code

    os.chdir('../foo')
    with monkeypatch.context() as m:
        from renku.api.storage import StorageApiMixin
        m.setattr(StorageApiMixin, 'external_storage_installed', False)

        result = runner.invoke(
            cli.cli, ['dataset', 'add', 'f', '../woop'],
            catch_exceptions=False
        )
        assert 1 == result.exit_code
        subprocess.call(['git', 'clean', '-dff'])

    result = runner.invoke(
        cli.cli, ['-S', 'dataset', 'add', 'f', '../woop'],
        catch_exceptions=False
    )
    assert 0 == result.exit_code

    os.chdir('../bar')
    result = runner.invoke(
        cli.cli, ['-S', 'dataset', 'add', 'b', '../foo/data/f/woop'],
        catch_exceptions=False
    )
    assert 0 == result.exit_code

    # Produce a derived data from the imported data.
    with open('woop.wc', 'w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.cli.main(
                    args=('-S', 'run', 'wc', 'data/b/data/f/woop'),
                    prog_name=runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert 0 == result.exit_code

    # Modify the source data.
    os.chdir('../foo')
    with open('data/f/woop', 'w') as f:
        f.write('woop2')

    subprocess.call(['git', 'commit', '-am', 'commiting changes to woop'])

    os.chdir('../bar')
    subprocess.call(['git', 'submodule', 'update', '--rebase', '--remote'])
    subprocess.call(['git', 'commit', '-am', 'update submodule'])

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert 0 != result.exit_code

    # Test relative log output
    cmd = ['--path', '../foo', 'log']
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert '../foo/data/f/woop' in result.output
    assert 0 == result.exit_code


def test_unchanged_output(runner, project):
    """Test detection of unchanged output."""
    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code

    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert 1 == result.exit_code


def test_unchanged_stdout(runner, project, capsys):
    """Test detection of unchanged stdout."""
    with capsys.disabled():
        with open('output.txt', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.cli.main(args=('run', 'echo', '1'), )
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
                    cli.cli.main(args=('run', 'echo', '1'), )
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

    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    # The output file is copied from the source.
    with output.open('r') as f:
        assert f.read().strip() == '1'

    update_source('2')

    # The input file has been updated and output is recreated.
    result = runner.invoke(cli.cli, cmd)
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
    from renku.api import LocalClient

    client = LocalClient(project)
    repo = client.repo
    greeting = client.path / 'greeting.txt'

    assert 0 == run(args=('run', 'echo', 'hello'), stdout=greeting)

    cmd = ['status']
    result = runner.invoke(cli.cli, cmd)
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
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code


def test_siblings(runner, project):
    """Test detection of siblings."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    for sibling in siblings:
        cmd = ['show', 'siblings', sibling]
        result = runner.invoke(cli.cli, cmd)
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
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    cmd = ['show', 'siblings', 'orphan.txt']
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code
    assert 'orphan.txt\n' == result.output


def test_only_child(runner, project):
    """Test detection of an only child."""
    cmd = ['run', 'touch', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    cmd = ['show', 'siblings', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code
    assert 'only_child\n' == result.output


def test_outputs(runner, project):
    """Test detection of outputs."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['show', 'outputs'])
    assert 0 == result.exit_code
    assert siblings == set(result.output.strip().split('\n'))


def test_simple_rerun(runner, project, run):
    """Test simple file recreation."""
    greetings = {'hello', 'hola', 'ahoj'}

    cwd = Path(project)
    source = cwd / 'source.txt'
    selected = cwd / 'selected.txt'

    repo = git.Repo(project)

    with source.open('w') as f:
        f.write('\n'.join(greetings))

    repo.git.add('--all')
    repo.index.commit('Created greetings')

    cmd = [
        'run', 'python', '-S', '-c',
        'import sys, random; print(random.choice(sys.stdin.readlines()))'
    ]

    assert 0 == run(cmd, stdin=source, stdout=selected)

    with selected.open('r') as f:
        greeting = f.read().strip()
        assert greeting in greetings

    def _rerun():
        """Return greeting after reruning."""
        assert 0 == run(args=('rerun', str(selected)))
        with selected.open('r') as fp:
            greeting = fp.read().strip()
            assert greeting in greetings
            return greeting

    for _ in range(100):
        new_greeting = _rerun()
        if greeting != new_greeting:
            break

    assert greeting != new_greeting, 'Something is not random'

    for _ in range(100):
        new_greeting = _rerun()
        if greeting == new_greeting:
            break

    assert greeting == new_greeting, 'Something is not random'


def test_rerun_with_inputs(runner, project, run):
    """Test file recreation with specified inputs."""
    cwd = Path(project)
    first = cwd / 'first.txt'
    second = cwd / 'second.txt'
    inputs = (first, second)

    output = cwd / 'output.txt'

    cmd = [
        'run', 'python', '-S', '-c', 'import random; print(random.random())'
    ]

    for file_ in inputs:
        assert 0 == run(args=cmd, stdout=file_), 'Random number generation.'

    cmd = ['run', 'cat'] + [str(path) for path in inputs]
    assert 0 == run(args=cmd, stdout=output)

    with output.open('r') as f:
        initial_data = f.read()

    assert 0 == run(args=('rerun', str(output)))

    with output.open('r') as f:
        assert f.read() != initial_data, 'The output should have changed.'

    # Keep the first file unchanged.
    with first.open('r') as f:
        first_data = f.read()

    assert 0 == run(args=('rerun', '--from', str(first), str(output)))

    with output.open('r') as f:
        assert f.read().startswith(first_data)


def test_rerun_with_edited_inputs(runner, project, run):
    """Test input modification."""
    cwd = Path(project)
    data = cwd / 'examples'
    data.mkdir()
    first = data / 'first.txt'
    second = data / 'second.txt'
    third = data / 'third.txt'

    run(args=['run', 'echo', 'hello'], stdout=first)
    run(args=['run', 'cat', str(first)], stdout=second)
    run(args=['run', 'echo', '1'], stdout=third)

    with first.open('r') as first_fp:
        with second.open('r') as second_fp:
            assert first_fp.read() == second_fp.read()

    # Change the initial input from "hello" to "hola".
    from click.testing import make_input_stream
    stdin = make_input_stream('hola\n', 'utf-8')
    assert 0 == run(args=('rerun', '--edit-inputs', str(second)), stdin=stdin)

    with second.open('r') as second_fp:
        assert 'hola\n' == second_fp.read()

    # Change the input from examples/first.txt to examples/third.txt.
    stdin = make_input_stream(str(third.name), 'utf-8')
    old_dir = os.getcwd()
    try:
        # Make sure the input path is relative to the current directory.
        os.chdir(str(data))

        result = runner.invoke(
            cli.cli,
            ['rerun', '--show-inputs', '--from',
             str(first),
             str(second)],
            catch_exceptions=False
        )
        assert 0 == result.exit_code
        assert 'input_1: {0}\n'.format(first.name) == result.output

        assert 0 == run(
            args=('rerun', '--edit-inputs', '--from', str(first), str(second)),
            stdin=stdin
        )
    finally:
        os.chdir(old_dir)

    with third.open('r') as third_fp:
        with second.open('r') as second_fp:
            assert third_fp.read() == second_fp.read()


@pytest.mark.skipif(
    shutil.which('docker') is None, reason='requires docker command line'
)
def test_image_pull(runner, project):
    """Test image pulling."""
    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert 1 == result.exit_code

    repo = git.Repo(project)
    origin = repo.create_remote('origin', project)
    origin.fetch()
    repo.heads.master.set_tracking_branch(origin.refs.master)

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert 1 == result.exit_code

    subprocess.call([
        'git', 'config', 'remote.origin.url', 'http://demo:demo@example.com'
    ])

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert 1 == result.exit_code

    subprocess.call([
        'git', 'config', 'remote.origin.url',
        'http://gitlab.com/example/example.git'
    ])

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert 'registry.gitlab.com/example/example' in result.output
    assert 'registry.gitlab.com/example/example.git' not in result.output
    assert 1 == result.exit_code

    subprocess.call([
        'git', 'config', 'remote.origin.url',
        'http://demo:demo@gitlab.example.com/repo.git'
    ])

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'registry.example.com/repo' in result.output
    assert 'registry.example.com/repo.git' not in result.output
    assert 1 == result.exit_code

    result = runner.invoke(
        cli.cli, ['config', 'registry', 'http://demo:demo@global.example.com']
    )
    assert 0 == result.exit_code

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'global.example.com' in result.output
    assert 1 == result.exit_code

    result = runner.invoke(
        cli.cli,
        ['config', 'origin.registry', 'http://demo:demo@origin.example.com']
    )
    assert 0 == result.exit_code

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'origin.example.com' in result.output
    assert 1 == result.exit_code


@pytest.mark.parametrize('cmd, exit_code', (('update', 0), ('rerun', 1)))
def test_input_update_and_rerun(cmd, exit_code, runner, project, run):
    """Test update and rerun of an input."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    assert exit_code == run(args=(cmd, input_.name))


def test_moved_file(runner, project):
    """Test that moved files are displayed correctly."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    result = runner.invoke(cli.cli, ['log'])
    assert 0 == result.exit_code
    assert input_.name in result.output

    repo.git.mv(input_.name, 'renamed.txt')
    repo.index.commit('Renamed input')

    result = runner.invoke(cli.cli, ['log'])
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
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code
    assert not input_.exists()
    assert Path('input.mv').exists()


def test_output_directory(runner, project, run):
    """Test detection of output directory."""
    cwd = Path(project)
    data = cwd / 'source' / 'data.txt'
    source = data.parent
    source.mkdir(parents=True)
    data.write_text('data')

    # Empty destination
    destination = cwd / 'destination'
    source_wc = cwd / 'destination_source.wc'
    # Non empty destination
    invalid_destination = cwd / 'invalid_destination'
    invalid_destination.mkdir(parents=True)
    (invalid_destination / 'non_empty').touch()

    repo = git.Repo(project)
    repo.git.add('--all')
    repo.index.commit('Created source directory')

    cmd = ['run', 'cp', '-LRf', str(source), str(destination)]
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code

    destination_source = destination / data.name
    assert destination_source.exists()

    # check that the output in subdir is added to LFS
    with (cwd / '.gitattributes').open() as f:
        gitattr = f.read()
    assert str(destination.relative_to(cwd)) + '/**' in gitattr
    assert destination_source.name in subprocess.check_output([
        'git', 'lfs', 'ls-files'
    ]).decode()

    cmd = ['run', 'wc']
    assert 0 == run(args=cmd, stdin=destination_source, stdout=source_wc)

    # Make sure the output directory can be recreated
    assert 0 == run(args=('rerun', str(source_wc)))
    assert {data.name} == {path.name for path in destination.iterdir()}

    cmd = ['log', str(source_wc)]
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    destination_data = str(Path('destination') / 'data.txt')
    assert destination_data in result.output, cmd
    assert ' directory)' in result.output

    cmd = ['run', 'cp', '-r', str(source), str(invalid_destination)]
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert 1 == result.exit_code
    assert not (invalid_destination / data.name).exists()


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

    result = runner.invoke(cli.cli, ['show', 'inputs'])
    assert set(
        str(p.relative_to(cwd))
        for p in inputs.rglob('*') if p.name != '.gitkeep'
    ) == set(result.output.strip().split('\n'))
