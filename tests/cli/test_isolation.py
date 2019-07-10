# -*- coding: utf-8 -*-
#
# Copyright 2018-2019- Swiss Data Science Center (SDSC)
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
"""Test execution of commands in parallel."""

import pytest

from renku._compat import Path


def test_run_in_isolation(runner, project, client, run):
    """Test run in isolation."""
    import filelock

    cwd = Path(project)
    with client.commit():
        with (cwd / '.gitignore').open('a') as f:
            f.write('lock')

    prefix = [
        'run',
        '--no-output',
    ]
    cmd = [
        'python', '-S', '-c',
        'import os, sys; sys.exit(1 if os.path.exists("lock") else 0)'
    ]

    head = client.repo.head.commit.hexsha

    with filelock.FileLock('lock'):
        assert 1 == run(args=prefix + cmd)
        assert client.repo.head.commit.hexsha == head

        assert 0 == run(prefix + ['--isolation'] + cmd)
        assert client.repo.head.commit.hexsha != head


@pytest.mark.shelled
def test_clean_isolated_run(tmpdir, runner, project, client, run, run_shell):
    """Test run in isolation."""
    script = client.path / 'script.py'
    output = Path('output')

    with client.commit():
        with script.open('w') as fp:
            fp.write(('import sys\n' 'print(sys.stdin.read())\n'))

    previous = client.repo.head.commit

    cmd = 'renku run --isolation python {0} > {1}'.format(script.name, output)
    ps = run_shell(cmd, return_ps=True)
    assert 0 != ps.pid

    # Commit the change to script.py on master.
    with client.commit():
        with script.open('w') as fp:
            fp.write('print("edited")')

    ps_output = ps.communicate(input=b'test')
    assert 0 == ps.wait()

    assert b'' == ps_output[0]
    assert ps_output[1] is None

    assert 'edited' == output.read_text().strip()

    diff = previous.diff(client.repo.head.commit)
    modifications = [
        modification
        for modification in diff if modification.change_type == 'M'
    ]
    assert 1 == len(modifications)


@pytest.mark.shelled
def test_dirty_tracked_isolated_run(
    tmpdir, runner, project, client, run, run_shell
):
    """Test run in isolation with dirty parent."""
    script = client.path / 'script.py'
    output = Path('output')

    with client.commit():
        with script.open('w') as fp:
            fp.write(('import sys\n' 'print(sys.stdin.read())\n'))

    dirty = client.path / 'dirty'
    with dirty.open('w') as fd:
        fd.write('dirty')

    cmd = 'renku run --isolation python {} > {}'.format(script.name, output)
    ps = run_shell(cmd, return_ps=True)
    assert 0 != ps.pid

    # Commit the change to script.py on master.
    dirty.unlink()
    with client.commit():
        with script.open('w') as fp:
            fp.write('print("edited")')

    dirty = client.path / 'dirty'
    with dirty.open('w') as fd:
        fd.write('dirty')

    ps_output = ps.communicate(input=b'test')
    assert 0 == ps.wait()

    assert b'' == ps_output[0]
    assert ps_output[1] is None

    assert 'edited' == output.read_text().strip()
    assert dirty.name in client.repo.untracked_files


@pytest.mark.shelled
def test_dirty_staged_isolated_run(
    tmpdir, runner, project, client, run, run_shell
):
    """Test run in isolation with dirty parent (contains staged files)."""
    script = client.path / 'script.py'
    output = Path('output')

    with client.commit():
        with script.open('w') as fp:
            fp.write(('import sys\n' 'print(sys.stdin.read())\n'))

    dirty = client.path / 'dirty'
    with dirty.open('w') as fd:
        fd.write('dirty')

    client.repo.git.add(dirty)

    cmd = 'renku run --isolation python {} > {}'.format(script.name, output)
    ps = run_shell(cmd, return_ps=True)
    assert 0 != ps.pid

    output = ps.communicate(input=b'test')
    assert 0 == ps.wait()

    assert b'' == output[0]
    assert output[1] is None

    diffs = client.repo.index.diff('HEAD')
    assert 1 == len(diffs)
    assert dirty.name == diffs[0].a_path
