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
"""Test Git subtrees."""

from renku import cli


def test_imported_data_repository(data_repository, runner, client, run):
    """Include data repository as subtree."""
    branch = data_repository.create_head('test_subtree', 'HEAD~2')
    client.repo.git.subtree(
        'add',
        # TODO '--squash',
        '--prefix',
        'data',
        data_repository.working_dir,
        branch.name,
    )

    assert 0 == run(
        args=('run', 'wc', '-c'),
        stdin=client.path / 'data' / 'file',
        stdout=client.path / 'file.wc',
    )

    with (client.path / 'file.wc').open('r') as fp:
        assert '4' == fp.read().strip()

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert 0 == result.exit_code, result.output

    result = runner.invoke(cli.cli, ['log'], catch_exceptions=False)
    # FIXME assert os.path.join('data', 'file') in result.output
    assert 'file\n' in result.output

    branch.set_commit('HEAD')
    client.repo.git.set_persistent_git_options(c='merge.ff=0')
    client.repo.git.subtree(
        'pull',
        # TODO '--squash',
        '--prefix',
        'data',
        data_repository.working_dir,
        branch.name,
    )

    result = runner.invoke(cli.cli, ['status'], catch_exceptions=False)
    assert 1 == result.exit_code, result.output
    assert 0 == run(args=('update', 'file.wc'))

    with (client.path / 'file.wc').open('r') as fp:
        assert '5' == fp.read().strip()

    result = runner.invoke(cli.cli, ['log'], catch_exceptions=False)
    # assert os.path.join('data', 'file') in result.output
    assert 'file\n' in result.output
