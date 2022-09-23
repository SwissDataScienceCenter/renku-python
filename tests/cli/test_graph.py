# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Test ``graph`` command."""

import os

import pytest

from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.domain_model.dataset import Url
from renku.ui.cli import cli
from tests.utils import format_result_exception, modified_environ, with_dataset


@pytest.mark.parametrize("revision", ["", "HEAD", "HEAD^", "HEAD^..HEAD"])
def test_graph_export_validation(runner, repository, directory_tree, run, revision):
    """Test graph validation when exporting."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-data", str(directory_tree)]).exit_code

    file1 = repository.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = repository.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict", "--revision", revision])

    assert 0 == result.exit_code, format_result_exception(result)

    assert "https://localhost" in result.output
    assert "https://renkulab.io" not in result.output

    with modified_environ(RENKU_DOMAIN="https://renkulab.io"):
        result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict", "--revision", revision])

        assert 0 == result.exit_code, format_result_exception(result)

        assert "https://localhost" not in result.output
        assert "https://renkulab.io" in result.output

    # Make sure that nothing has changed during export which is a read-only operation
    assert not repository.is_dirty(untracked_files=True)


@pytest.mark.serial
@pytest.mark.shelled
def test_graph_export_strict_run(runner, project, run_shell):
    """Test graph export output of run command."""
    # Run a shell command with pipe.
    assert run_shell('renku run --name run1 echo "my input string" > my_output_file')[1] is None
    assert run_shell("renku run --name run2 cp my_output_file my_output_file2")[1] is None
    assert run_shell("renku workflow compose my-composite-plan run1 run2")[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--full", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my_output_file" in result.output
    assert "my input string" in result.output
    assert "my_output_file2" in result.output
    assert "my-composite-plan" in result.output

    assert run_shell("renku workflow remove composite")[1] is None
    assert run_shell("renku workflow remove run2")[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_graph_export_strict_dataset(tmpdir, runner, project, subdirectory):
    """Test output of graph export for dataset add."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    paths = []
    test_paths = []
    for i in range(3):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        paths.append(str(new_file))
        test_paths.append(os.path.relpath(str(new_file), str(project.path)))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert all(p in result.output for p in test_paths), result.output

    # check that only most recent dataset is exported
    assert 1 == result.output.count("http://schema.org/Dataset")

    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld", "--full"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert all(p in result.output for p in test_paths), result.output

    # check that all datasets are exported
    assert 2 == result.output.count("http://schema.org/Dataset")


def test_graph_export_dataset_mutability(runner, project_with_datasets, client_database_injection_manager):
    """Test export validation fails for datasets that have both same_as and derived_from."""
    with client_database_injection_manager(project_with_datasets):
        with with_dataset(project_with_datasets, name="dataset-1", commit_database=True) as dataset:
            # NOTE: Set both same_as and derived_from for a dataset
            dataset.same_as = Url(url_str="http://example.com")
            dataset.derived_from = Url(url_id="datasets/abc123")

    result = runner.invoke(cli, ["graph", "export", "--full", "--strict"])

    assert 1 == result.exit_code
    assert "Both prov:wasDerivedFrom and schema:sameAs are set." in result.output
