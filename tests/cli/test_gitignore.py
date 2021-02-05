# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Test compliance with ``.gitignore`` file."""
from renku.cli import cli


def test_dataset_add(tmpdir, runner, client, subdirectory):
    """Test importing data into a dataset."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "testing"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # Using an extension from gitignore.default defined as *.spec
    ignored_file = tmpdir.join("my.spec")
    ignored_file.write("My Specification")

    # The file should be ignored and command fail
    result = runner.invoke(cli, ["dataset", "add", "testing", ignored_file.strpath], catch_exceptions=False,)

    assert 1 == result.exit_code

    client.repo.git.clean("-dff")

    # Use the --force ;)
    result = runner.invoke(cli, ["dataset", "add", "testing", "--force", ignored_file.strpath])
    assert 0 == result.exit_code
