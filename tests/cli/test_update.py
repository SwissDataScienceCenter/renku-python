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
"""Test update functionality."""

import git

from renku import cli
from renku._compat import Path


def test_update(runner, project, run):
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

    assert 0 == run(args=('run', 'wc', '-c'), stdin=source, stdout=output)

    with output.open('r') as f:
        assert f.read().strip() == '1'

    result = runner.invoke(cli.cli, ['status'])
    assert 0 == result.exit_code

    update_source('12')

    result = runner.invoke(cli.cli, ['status'])
    assert 1 == result.exit_code

    assert 0 == run()

    result = runner.invoke(cli.cli, ['status'])
    assert 0 == result.exit_code

    with output.open('r') as f:
        assert f.read().strip() == '2'

    result = runner.invoke(cli.cli, ['log'], catch_exceptions=False)
    assert '(part of' in result.output, result.output

    # Source has been updated but output is unchanged.
    update_source('34')

    result = runner.invoke(cli.cli, ['status'])
    assert 1 == result.exit_code

    assert 0 == run()

    result = runner.invoke(cli.cli, ['status'])
    assert 0 == result.exit_code

    with output.open('r') as f:
        assert f.read().strip() == '2'

    from renku.cli.log import FORMATS
    for output_format in FORMATS:
        # Make sure the log contains the original parent.
        result = runner.invoke(
            cli.cli,
            ['log', '--format', output_format],
            catch_exceptions=False,
        )
        assert 0 == result.exit_code, output_format
        assert source.name in result.output, output_format


def test_workflow_without_outputs(runner, project, run):
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
    assert 0 == result.exit_code

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code

    with input_.open('w') as f:
        f.write('second')

    repo.git.add('--all')
    repo.index.commit('Updated input.txt')

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert 1 == result.exit_code

    assert 0 == run(args=('update', '--no-output'))

    cmd = ['status', '--no-output']
    result = runner.invoke(cli.cli, cmd)
    assert 0 == result.exit_code


def test_siblings_update(runner, project, run):
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
    assert 0 == run(args=cmd, stdin=parent, stdout=sister)

    # The output file is copied from the source.
    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '1', sibling

    update_source('2')

    # Siblings must be updated together.
    for sibling in siblings:
        assert 1 == run(args=('update', sibling.name))

    # Update brother and check the sister has not been changed.
    assert 0 == run(args=('update', '--with-siblings', brother.name))

    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '2', sibling

    update_source('3')

    # Siblings kept together even when one is removed.
    repo.index.remove([brother.name], working_tree=True)
    repo.index.commit('Brother removed')

    assert not brother.exists()

    # Update should find also missing siblings.
    assert 1 == run(args=('update', ))
    assert 0 == run(args=('update', '--with-siblings'))

    for sibling in siblings:
        with sibling.open('r') as f:
            assert f.read().strip() == '3', sibling


def test_siblings_in_output_directory(runner, project, run):
    """Files in output directory are linked or removed after update."""
    repo = git.Repo(project)
    cwd = Path(project)
    source = cwd / 'source.txt'
    output = cwd / 'output'

    files = [
        ('first', '1'),
        ('second', '2'),
        ('third', '3'),
    ]

    def write_source():
        """Write source from files."""
        with source.open('w') as fp:
            fp.write('\n'.join(' '.join(line) for line in files) + '\n')

        repo.git.add('--all')
        repo.index.commit('Update source.txt')

    def check_files():
        """Check file content."""
        assert len(files) == len(list(output.rglob('*')))

        for name, content in files:
            with (output / name).open() as fp:
                assert content == fp.read().strip(), name

    write_source()

    script = (
        'mkdir -p "$0"; '
        'cat - | while read -r name content; do '
        'echo "$content" > "$0/$name"; done'
    )
    base_sh = ['sh', '-c', script, 'output']

    assert not output.exists()
    assert 0 == run(args=['run'] + base_sh + ['output'], stdin=source)
    assert output.exists()
    check_files()

    files = [
        ('first', '11'),
        ('third', '3'),
        ('fourth', '4'),
    ]
    write_source()

    assert 0 == run(args=['update', 'output'])
    check_files()
