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

    # Compare default log and log for a specific file.
    result_default = runner.invoke(cli.cli, ['log'])
    result_arg = runner.invoke(cli.cli, ['log', 'counted.txt'])

    assert result_default.exit_code == 0
    assert result_arg.exit_code == 0
    assert result_default.output == result_arg.output


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

    with open('source.txt', 'w') as source:
        source.write('first,second,third,fourth')

    repo.git.add('--all')
    repo.index.commit('Changed source.txt')

    result = runner.invoke(cli.cli, ['status'])
    assert result.exit_code == 1
    assert 'source.txt' in result.output


def test_streams_cleanup(project, runner, capsys):
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


def test_update(project, runner, capsys):
    """Test automatic file update."""
    cwd = Path(project)
    data = cwd / 'data'
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
        with open(source, 'rb') as stdin:
            with open(output, 'wb') as stdout:
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
    result = runner.invoke(cli.cli, ['log'])
    assert source.name in result.output


def test_streams_and_args_names(runner, capsys):
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


def test_multiple_file_to_dataset(tmpdir, data_repository, runner):
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


def test_relative_import_to_dataset(tmpdir, data_repository, runner):
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


def test_relative_git_import_to_dataset(tmpdir, project, runner):
    """Test importing data from a directory structure."""
    submodule_name = os.path.basename(tmpdir)

    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/metadata.yml')

    data_repo = git.Repo.init(tmpdir)

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


def test_unchanged_output(runner):
    """Test detection of unchanged output."""
    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    cmd = ['run', 'touch', '1']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 1


def test_unchanged_stdout(runner, capsys):
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


def test_modified_output(project, runner, capsys):
    """Test detection of changed file as output."""
    cwd = Path(project)
    source = cwd / 'source.txt'
    output = cwd / 'result.txt'

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


def test_siblings(runner):
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


def test_orphan(project, runner):
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


def test_only_child(runner):
    """Test detection of an only child."""
    cmd = ['run', 'touch', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    cmd = ['show', 'siblings', 'only_child']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0
    assert 'only_child\n' == result.output


def test_siblings_update(project, runner, capsys):
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


def test_simple_rerun(project, runner, capsys):
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


def test_rerun_with_inputs(project, runner, capsys):
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
