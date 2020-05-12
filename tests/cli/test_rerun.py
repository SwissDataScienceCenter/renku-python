# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Test ``rerun`` command."""

from __future__ import absolute_import, print_function

import os
import subprocess
from pathlib import Path

import git
import pytest
from click.testing import CliRunner

from renku.cli import cli


def test_simple_rerun(runner, project, run, no_lfs_warning):
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


def test_rerun_with_edited_inputs(project, run, no_lfs_warning):
    """Test input modification."""
    runner = CliRunner(mix_stderr=False)

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
            cli, ['rerun', '--show-inputs', '--from',
                  str(first),
                  str(second)],
            catch_exceptions=False
        )
        assert 0 == result.exit_code
        assert 'input_1: {0}\n'.format(first.name) == result.stdout

        assert 0 == run(
            args=('rerun', '--edit-inputs', '--from', str(first), str(second)),
            stdin=stdin
        )
    finally:
        os.chdir(old_dir)

    with third.open('r') as third_fp:
        with second.open('r') as second_fp:
            assert third_fp.read() == second_fp.read()


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


def test_output_directory(runner, project, run, no_lfs_size_limit):
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
    repo.index.commit('Created source directory', skip_hooks=True)

    cmd = ['run', 'cp', '-LRf', str(source), str(destination)]
    result = runner.invoke(cli, cmd, catch_exceptions=False)
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
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    destination_data = str(Path('destination') / 'data.txt')
    assert destination_data in result.output, cmd
    assert ' directory)' in result.output

    cmd = ['run', 'cp', '-r', str(source), str(invalid_destination)]
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert 1 == result.exit_code
    assert not (invalid_destination / data.name).exists()
