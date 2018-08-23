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
from renku._compat import Path
from renku.models.cwl.workflow import Workflow


def _run_update(runner, capsys, args=('update', )):
    """Run the update command."""
    with capsys.disabled():
        try:
            cli.cli.main(
                args=args,
                prog_name=runner.get_default_prog_name(cli.cli),
            )
        except SystemExit as e:
            return 0 if e.code is None else e.code
        except Exception:
            raise


def test_version(runner):
    """Test cli version."""
    result = runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


@pytest.mark.parametrize('arg', (('help', ), ('-h', ), ('--help', )))
def test_help(arg, runner):
    """Test cli help."""
    result = runner.invoke(cli.cli, [arg])
    assert result.exit_code == 0
    assert 'Show this message and exit.' in result.output


def test_config_path(runner):
    """Test config path."""
    from renku.cli._config import RENKU_HOME

    result = runner.invoke(cli.cli, ['--config-path'])
    output = result.output.split('\n')[0]
    assert 'config.yml' in output
    assert RENKU_HOME in output


def test_init(isolated_runner):
    """Test project initialization."""
    runner = isolated_runner

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


def test_workon(runner, project):
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


def test_run_simple(runner, project):
    """Test tracking of run command."""
    cmd = ['echo', 'test']
    result = runner.invoke(cli.cli, ['run', '--no-output'] + cmd)
    assert result.exit_code == 0

    # There are no output files.
    result = runner.invoke(cli.cli, ['log'])
    assert result.output.strip() == ''

    # Display tools with no outputs.
    result = runner.invoke(cli.cli, ['log', '--no-output'])
    assert '.renku/workflow/' in result.output


def test_git_pre_commit_hook(runner, project, capsys):
    """Test detection of output edits."""
    result = runner.invoke(cli.cli, ['githooks', 'install'])
    assert result.exit_code == 0
    assert 'Hook already exists.' in result.output

    repo = git.Repo(project)
    cwd = Path(project)
    output = cwd / 'output.txt'

    result = runner.invoke(cli.cli, ['run', 'touch', output.name])
    assert result.exit_code == 0

    with output.open('w') as f:
        f.write('hello')

    repo.git.add('--all')
    with pytest.raises(git.HookExecutionError) as error:
        repo.index.commit('hello')
        assert output.name in error.stdout

    result = runner.invoke(cli.cli, ['githooks', 'uninstall'])
    assert result.exit_code == 0

    repo.index.commit('hello')


def test_workflow(runner, project):
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

    # Compare default log and log for a specific file.
    result_default = runner.invoke(cli.cli, ['log'])
    result_arg = runner.invoke(cli.cli, ['log', 'counted.txt'])

    assert result_default.exit_code == 0
    assert result_arg.exit_code == 0
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
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 0

    # Check that source.txt is not shown in outputs.
    result = runner.invoke(cli.cli, ['show', 'outputs', 'source.txt'])
    assert result.exit_code == 1

    result = runner.invoke(cli.cli, ['show', 'outputs'])
    assert result.exit_code == 0
    assert {
        'result.txt',
    } == set(result.output.strip().split('\n'))

    with open('source.txt', 'w') as source:
        source.write('first,second,third,fourth')

    repo.git.add('--all')
    repo.index.commit('Changed source.txt')

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1
    assert 'source.txt' in result.output


def test_streams_cleanup(runner, project, capsys):
    """Test cleanup of standard streams."""
    with open('source.txt', 'w') as source:
        source.write('first,second,third')

    # File outside the Git index should be deleted.
    with capsys.disabled():
        with open('result.txt', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.cli.main(
                        args=('run', 'cat', 'source.txt'),
                        prog_name=runner.get_default_prog_name(cli.cli),
                    )
                except SystemExit as e:
                    assert e.code in {None, 1}, 'The repo must be dirty.'
            finally:
                sys.stdout = old_stdout

    with open('source.txt', 'r') as source:
        assert source.read() == 'first,second,third'

    assert not Path('result.txt').exists()

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1

    # File from the Git index should be restored.
    repo = git.Repo(project)
    with open('result.txt', 'w') as fp:
        fp.write('1')

    repo.index.add(['result.txt'])

    with capsys.disabled():
        with open('result.txt', 'wb') as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.cli.main(
                        args=('run', 'cat', 'source.txt'),
                        prog_name=runner.get_default_prog_name(cli.cli),
                    )
                except SystemExit as e:
                    assert e.code in {None, 1}, 'The repo must be dirty.'
            finally:
                sys.stdout = old_stdout

    with open('result.txt', 'r') as fp:
        assert fp.read() == '1'


def test_update(runner, project, capsys):
    """Test automatic file update."""
    cwd = Path(project)
    data = cwd / 'data'
    data.mkdir()
    source = cwd / 'source.txt'
    output = data / 'result.txt'

    repo = git.Repo(project)

    def update_source(data):
        """Update source.txt."""
        with source.open('w') as fp:
            fp.write(data)

        repo.git.add('--all')
        repo.index.commit('Updated source.txt')

    update_source('1')

    with capsys.disabled():
        with source.open('rb') as stdin:
            with output.open('wb') as stdout:
                try:
                    old_stdin, old_stdout = sys.stdin, sys.stdout
                    sys.stdin, sys.stdout = stdin, stdout

                    try:
                        cli.cli.main(
                            args=('run', 'wc', '-c'),
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with output.open('r') as f:
        assert f.read().strip() == '1'

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 0

    update_source('12')

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1

    assert _run_update(runner, capsys) == 0

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 0

    with output.open('r') as f:
        assert f.read().strip() == '2'

    # Source has been updated but output is unchanged.
    update_source('34')

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1

    assert _run_update(runner, capsys) == 0

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 0

    with output.open('r') as f:
        assert f.read().strip() == '2'

    # Make sure the log contains the original parent.
    result = runner.invoke(
        cli.cli, ['log', '--format', 'dot'], catch_exceptions=False
    )
    assert source.name in result.output


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
    assert result.exit_code == 0


def test_datasets(data_file, data_repository, runner, project):
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
        os.path.join('data', 'dataset', os.path.basename(str(data_file)))
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


def test_multiple_file_to_dataset(tmpdir, data_repository, runner, project):
    """Test importing multiple data into a dataset at once."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/metadata.yml')

    paths = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli.cli, ['dataset', 'add', 'dataset'] + paths)
    assert result.exit_code == 0


def test_relative_import_to_dataset(tmpdir, data_repository, runner, project):
    """Test importing data from a directory structure."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/metadata.yml')

    zero_data = tmpdir.join('data.txt')
    zero_data.write('zero')

    first_level = tmpdir.mkdir('first')
    second_level = first_level.mkdir('second')

    first_data = first_level.join('data.txt')
    first_data.write('first')

    second_data = second_level.join('data.txt')
    second_data.write('second')

    paths = [str(zero_data), str(first_data), str(second_data)]

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset', '--relative-to',
         str(tmpdir)] + paths,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert os.stat(os.path.join('data', 'dataset', 'data.txt'))
    assert os.stat(os.path.join('data', 'dataset', 'first', 'data.txt'))
    assert os.stat(
        os.path.join('data', 'dataset', 'first', 'second', 'data.txt')
    )


def test_relative_git_import_to_dataset(tmpdir, runner, project):
    """Test importing data from a directory structure."""
    submodule_name = os.path.basename(str(tmpdir))

    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/metadata.yml')

    data_repo = git.Repo.init(str(tmpdir))

    zero_data = tmpdir.join('data.txt')
    zero_data.write('zero')

    first_level = tmpdir.mkdir('first')
    second_level = first_level.mkdir('second')

    first_data = first_level.join('data.txt')
    first_data.write('first')

    second_data = second_level.join('data.txt')
    second_data.write('second')

    paths = [str(zero_data), str(first_data), str(second_data)]
    data_repo.index.add(paths)
    data_repo.index.commit('Added source files')

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        [
            'dataset', 'add', 'dataset', '--relative-to',
            str(first_level),
            str(tmpdir)
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert os.stat(os.path.join('data', 'dataset', submodule_name, 'data.txt'))
    assert os.stat(
        os.path.join('data', 'dataset', submodule_name, 'second', 'data.txt')
    )

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'relative', '--relative-to', 'first',
         str(tmpdir)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert os.stat(
        os.path.join('data', 'relative', submodule_name, 'data.txt')
    )
    assert os.stat(
        os.path.join('data', 'relative', submodule_name, 'second', 'data.txt')
    )


def test_file_tracking(isolated_runner):
    """Test .gitattribute handling on renku run."""
    runner = isolated_runner

    os.mkdir('test-project')
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['run', 'touch', 'output'])
    assert result.exit_code == 0

    with open('.gitattributes') as f:
        gitattributes = f.read()
    assert 'output' in gitattributes


def test_status_with_submodules(isolated_runner):
    """Test status calculation with submodules."""
    runner = isolated_runner

    os.mkdir('foo')
    os.mkdir('bar')

    with open('woop', 'w') as f:
        f.write('woop')

    os.chdir('foo')
    result = runner.invoke(cli.cli, ['init', '-S'], catch_exceptions=False)
    assert result.exit_code == 0

    os.chdir('../bar')
    result = runner.invoke(cli.cli, ['init', '-S'], catch_exceptions=False)
    assert result.exit_code == 0

    os.chdir('../foo')
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'f', '../woop'], catch_exceptions=False
    )
    assert result.exit_code == 0

    os.chdir('../bar')
    result = runner.invoke(
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
                    prog_name=runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert result.exit_code == 0

    # Modify the source data.
    os.chdir('../foo')
    with open('data/f/woop', 'w') as f:
        f.write('woop2')

    call(['git', 'commit', '-am', 'commiting changes to woop'])

    os.chdir('../bar')
    call(['git', 'submodule', 'update', '--rebase', '--remote'])
    call(['git', 'commit', '-am', 'update submodule'])

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert result.exit_code != 0


def test_unchanged_output(runner, project):
    """Test detection of unchanged output."""
    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert result.exit_code == 0

    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert result.exit_code == 1


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


def test_modified_output(runner, project, capsys):
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
    assert result.exit_code == 0

    # The output file is copied from the source.
    with output.open('r') as f:
        assert f.read().strip() == '1'

    update_source('2')

    # The input file has been updated and output is recreated.
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    with output.open('r') as f:
        assert f.read().strip() == '2'

    update_source('3')

    # The input has been modifed and we check that the previous
    # run command correctly recognized output.txt.
    assert _run_update(runner, capsys) == 0

    with output.open('r') as f:
        assert f.read().strip() == '3'


def test_siblings(runner, project):
    """Test detection of siblings."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    for sibling in siblings:
        cmd = ['show', 'siblings', sibling]
        result = runner.invoke(cli.cli, cmd)
        assert result.exit_code == 0

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
    assert result.exit_code == 0

    cmd = ['show', 'siblings', 'orphan.txt']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0
    assert 'orphan.txt\n' == result.output


def test_only_child(runner, project):
    """Test detection of an only child."""
    cmd = ['run', 'touch', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    cmd = ['show', 'siblings', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0
    assert 'only_child\n' == result.output


def test_outputs(runner, project):
    """Test detection of outputs."""
    siblings = {'brother', 'sister'}

    cmd = ['run', 'touch'] + list(siblings)
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['show', 'outputs'])
    assert result.exit_code == 0
    assert siblings == set(result.output.strip().split('\n'))


def test_workflow_without_outputs(runner, project, capsys):
    """Test workflow without outputs."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    cmd = ['run', 'cat', '--no-output', input_.name]
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    with input_.open('w') as f:
        f.write('second')

    repo.git.add('--all')
    repo.index.commit('Updated input.txt')

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 1

    assert 0 == _run_update(runner, capsys, args=('update', '--no-output'))

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0


def test_siblings_update(runner, project, capsys):
    """Test detection of siblings during update."""
    cwd = Path(project)
    parent = cwd / 'parent.txt'
    brother = cwd / 'brother.txt'
    sister = cwd / 'sister.txt'
    siblings = {brother, sister}

    repo = git.Repo(project)

    def update_source(data):
        """Update parent.txt."""
        with parent.open('w') as fp:
            fp.write(data)

        repo.git.add('--all')
        repo.index.commit('Updated parent.txt')

    update_source('1')

    # The output files do not exist.
    assert not any(sibling.exists() for sibling in siblings)

    cmd = ['run', 'tee', 'brother.txt']

    with capsys.disabled():
        with parent.open('rb') as stdin:
            with sister.open('wb') as stdout:
                try:
                    old_stdin, old_stdout = sys.stdin, sys.stdout
                    sys.stdin, sys.stdout = stdin, stdout
                    try:
                        cli.cli.main(
                            args=cmd,
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    # The output file is copied from the source.
    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '1', sibling

    update_source('2')

    # Siblings must be updated together.
    for sibling in siblings:
        assert 1 == _run_update(runner, capsys, args=('update', sibling.name))

    # Update brother and check the sister has not been changed.
    assert 0 == _run_update(
        runner, capsys, args=('update', '--with-siblings', brother.name)
    )

    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '2', sibling

    update_source('3')

    # Siblings kept together even when one is removed.
    repo.index.remove([brother.name], working_tree=True)
    repo.index.commit('Brother removed')

    assert not brother.exists()

    # Update should find also missing siblings.
    assert 1 == _run_update(runner, capsys, args=('update', ))
    assert 0 == _run_update(runner, capsys, args=('update', '--with-siblings'))

    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '3', sibling


def test_simple_rerun(runner, project, capsys):
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

    with capsys.disabled():
        with source.open('rb') as stdin:
            with selected.open('wb') as stdout:
                try:
                    old_stdin, old_stdout = sys.stdin, sys.stdout
                    sys.stdin, sys.stdout = stdin, stdout
                    try:
                        cli.cli.main(
                            args=cmd,
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with selected.open('r') as f:
        greeting = f.read().strip()
        assert greeting in greetings

    def _rerun():
        """Return greeting after reruning."""
        assert 0 == _run_update(runner, capsys, args=('rerun', str(selected)))
        with selected.open('r') as f:
            greeting = f.read().strip()
            assert greeting in greetings
            return greeting

    for _ in range(100):
        new_greeting = _rerun()
        if greeting != new_greeting:
            break

    assert greeting != new_greeting, "Something is not random"

    for _ in range(100):
        new_greeting = _rerun()
        if greeting == new_greeting:
            break

    assert greeting == new_greeting, "Something is not random"


def test_rerun_with_inputs(runner, project, capsys):
    """Test file recreation with specified inputs."""
    cwd = Path(project)
    first = cwd / 'first.txt'
    second = cwd / 'second.txt'
    inputs = (first, second)

    output = cwd / 'output.txt'

    cmd = [
        'run', 'python', '-S', '-c', 'import random; print(random.random())'
    ]

    def _generate(output, cmd):
        """Generate an output."""
        with capsys.disabled():
            with output.open('wb') as stdout:
                try:
                    old_stdout = sys.stdout
                    sys.stdout = stdout
                    try:
                        cli.cli.main(
                            args=cmd,
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdout = old_stdout

    for file_ in inputs:
        _generate(file_, cmd)

    cmd = ['run', 'cat'] + [str(path) for path in inputs]
    _generate(output, cmd)

    with output.open('r') as f:
        initial_data = f.read()

    assert 0 == _run_update(runner, capsys, args=('rerun', str(output)))

    with output.open('r') as f:
        assert f.read() != initial_data, "The output should have changed."

    # Keep the first file unchanged.
    with first.open('r') as f:
        first_data = f.read()

    assert 0 == _run_update(
        runner, capsys, args=('rerun', '--from', str(first), str(output))
    )

    with output.open('r') as f:
        assert f.read().startswith(first_data)


def test_rerun_with_edited_inputs(runner, project, capsys):
    """Test input modification."""
    cwd = Path(project)
    data = cwd / 'examples'
    data.mkdir()
    first = data / 'first.txt'
    second = data / 'second.txt'
    third = data / 'third.txt'

    def _generate(output, cmd):
        """Generate an output."""
        with capsys.disabled():
            with output.open('wb') as stdout:
                try:
                    old_stdout = sys.stdout
                    sys.stdout = stdout
                    try:
                        cli.cli.main(
                            args=cmd,
                            prog_name=runner.get_default_prog_name(cli.cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdout = old_stdout

    _generate(first, ['run', 'echo', 'hello'])
    _generate(second, ['run', 'cat', str(first)])
    _generate(third, ['run', 'echo', '1'])

    with first.open('r') as first_fp:
        with second.open('r') as second_fp:
            assert first_fp.read() == second_fp.read()

    # Change the initial input from "hello" to "hola".
    from click.testing import make_input_stream
    stdin = make_input_stream('hola\n', 'utf-8')
    old_stdin = sys.stdin
    try:
        sys.stdin = stdin
        assert 0 == _run_update(
            runner, capsys, args=('rerun', '--edit-inputs', str(second))
        )
    finally:
        sys.stdin = old_stdin

    with second.open('r') as second_fp:
        assert 'hola\n' == second_fp.read()

    # Change the input from examples/first.txt to examples/third.txt.
    stdin = make_input_stream(str(third.name), 'utf-8')
    old_stdin = sys.stdin
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

        sys.stdin = stdin
        assert 0 == _run_update(
            runner,
            capsys,
            args=('rerun', '--edit-inputs', '--from', str(first), str(second))
        )
    finally:
        os.chdir(old_dir)
        sys.stdin = old_stdin

    with third.open('r') as third_fp:
        with second.open('r') as second_fp:
            assert third_fp.read() == second_fp.read()


@pytest.mark.skipif(
    shutil.which('docker') is None, reason="requires docker command line"
)
def test_image_pull(runner, project):
    """Test image pulling."""
    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 1

    repo = git.Repo(project)
    origin = repo.create_remote('origin', project)
    origin.fetch()
    repo.heads.master.set_tracking_branch(origin.refs.master)

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 1

    call([
        'git', 'config', 'remote.origin.url', 'http://demo:demo@example.com'
    ])

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 1

    call([
        'git', 'config', 'remote.origin.url',
        'http://gitlab.com/example/example.git'
    ])

    cmd = ['image', 'pull']
    result = runner.invoke(cli.cli, cmd)
    assert 'registry.gitlab.com/example/example' in result.output
    assert 'registry.gitlab.com/example/example.git' not in result.output
    assert result.exit_code == 1

    call([
        'git', 'config', 'remote.origin.url',
        'http://demo:demo@gitlab.example.com/repo.git'
    ])

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'registry.example.com/repo' in result.output
    assert 'registry.example.com/repo.git' not in result.output
    assert result.exit_code == 1

    result = runner.invoke(
        cli.cli, ['config', 'registry', 'http://demo:demo@global.example.com']
    )
    assert result.exit_code == 0

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'global.example.com' in result.output
    assert result.exit_code == 1

    result = runner.invoke(
        cli.cli,
        ['config', 'origin.registry', 'http://demo:demo@origin.example.com']
    )
    assert result.exit_code == 0

    cmd = ['image', 'pull', '--no-auto-login']
    result = runner.invoke(cli.cli, cmd)
    assert 'origin.example.com' in result.output
    assert result.exit_code == 1


def test_input_update_and_rerun(runner, project, capsys):
    """Test update and rerun of an input."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / 'input.txt'
    with input_.open('w') as f:
        f.write('first')

    repo.git.add('--all')
    repo.index.commit('Created input.txt')

    assert 1 == _run_update(runner, capsys, args=('update', input_.name))
    assert 1 == _run_update(runner, capsys, args=('rerun', input_.name))


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
    assert result.exit_code == 0
    assert not input_.exists()
    assert Path('input.mv').exists()


def test_output_directory(runner, project):
    """Test detection of output directory."""
    cwd = Path(project)
    data = cwd / 'source' / 'data.txt'
    source = data.parent
    source.mkdir(parents=True)
    data.touch()

    # Empty destination
    destination = cwd / 'destination'
    # Non empty destination
    invalid_destination = cwd / 'invalid_destination'
    invalid_destination.mkdir(parents=True)
    (invalid_destination / 'non_empty').touch()

    repo = git.Repo(project)
    repo.git.add('--all')
    repo.index.commit('Created source directory')

    cmd = ['run', 'cp', '-r', str(source), str(destination)]
    result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    assert result.exit_code == 0
    assert (destination / data.name).exists()

    # FIXME the output directory MUST be empty.
    # cmd = ['run', 'cp', '-r', str(source), str(invalid_destination)]
    # result = runner.invoke(cli.cli, cmd, catch_exceptions=False)
    # assert result.exit_code == 1
    # assert not (invalid_destination / data.name).exists()


def test_input_directory(runner, project, capsys):
    """Test detection of input directory."""
    repo = git.Repo(project)
    cwd = Path(project)
    output = cwd / 'output.txt'
    inputs = cwd / 'inputs'
    inputs.mkdir(parents=True)
    (inputs / 'first').touch()

    repo.git.add('--all')
    repo.index.commit('Created inputs')

    with output.open('w') as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.cli.main(
                    args=('run', 'ls', str(inputs)),
                    prog_name=runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    with output.open('r') as f:
        assert 'first\n' == f.read()

    (inputs / 'second').touch()
    repo.git.add('--all')
    repo.index.commit('Added second input')

    assert 0 == _run_update(runner, capsys, args=('update', output.name))
    with output.open('r') as f:
        assert 'first\nsecond\n' == f.read()
