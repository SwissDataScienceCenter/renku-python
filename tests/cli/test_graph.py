#
# Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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

import pytest

from renku.domain_model.dataset import Url
from renku.ui.cli import cli
from tests.utils import format_result_exception, modified_environ, with_dataset


@pytest.mark.parametrize("revision", ["", "HEAD", "HEAD^", "HEAD^..HEAD"])
def test_graph_export_validation(runner, project_with_dataset_and_workflows, revision):
    """Test graph validation when exporting."""
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
    assert not project_with_dataset_and_workflows.repository.is_dirty()


@pytest.mark.serial
@pytest.mark.shelled
def test_graph_export_strict(
    runner,
    project_with_dataset_and_workflows,
    run_shell,
):
    """Test strict graph export output command."""

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--full", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my_output_file" in result.output
    assert "my input string" in result.output
    assert "my_output_file2" in result.output
    assert "my-composite-plan" in result.output

    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld", "--revision", "HEAD^^^^"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check that only most recent dataset is exported
    assert 1 == result.output.count("http://schema.org/Dataset")

    assert run_shell("renku workflow remove composite")[1] is None
    assert run_shell("renku workflow remove run2")[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert all(p in result.output for p in ["my_output_file2", "my_output_file"]), result.output

    assert 2 == result.output.count("http://schema.org/Dataset")

    # remove and readd dataset
    result = runner.invoke(cli, ["dataset", "rm", "my-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "create", "my-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--strict", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check that all datasets are exported
    assert 4 == result.output.count("http://schema.org/Dataset")
    assert 1 == result.output.count("invalidatedAtTime")


def test_graph_export_dataset_mutability(runner, project_with_datasets, with_injection):
    """Test export validation fails for datasets that have both same_as and derived_from."""
    with with_injection():
        with with_dataset(name="dataset-1", commit_database=True) as dataset:
            # NOTE: Set both same_as and derived_from for a dataset
            dataset.same_as = Url(url_str="http://example.com")
            dataset.derived_from = Url(url_id="datasets/abc123")

    result = runner.invoke(cli, ["graph", "export", "--full", "--strict"])

    assert 1 == result.exit_code
    assert "Both prov:wasDerivedFrom and schema:sameAs are set." in result.output
