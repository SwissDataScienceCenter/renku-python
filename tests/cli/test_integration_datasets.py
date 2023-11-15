# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Integration tests for dataset command."""

import os
import random
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import NamedTuple
from urllib import parse

import pytest

from renku.core import errors
from renku.core.config import get_value, remove_value, set_value
from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.interface.storage import FileHash
from renku.core.util.contexts import chdir
from renku.core.util.git import get_git_user
from renku.core.util.os import get_files, unmount_path
from renku.domain_model.dataset import Url
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from tests.utils import (
    assert_dataset_is_mutated,
    format_result_exception,
    get_dataset_with_injection,
    get_datasets_provenance_with_injection,
    retry_failed,
    with_dataset,
    write_and_commit_file,
)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "doi",
    [
        {
            "doi": "10.5281/zenodo.2658634",
            "slug": "pyndl_naive_discr_v0.6.4",
            "creator": "Konstantin Sering, Marc Weitz, David-Elias Künstle, Lennart Schneider",
            "version": "v0.6.4",
            "keywords": {
                "naive discriminative learning",
                "linguistics",
                "python",
                "cognitive science",
                "machine learning",
            },
        },
        {
            "doi": "10.7910/DVN/F4NUMR",
            "slug": "replication_data_for_2.2",
            "creator": "James Druckman, Martin Kifer, Michael Parkin",
            "version": "2",
            "keywords": {"Social Sciences"},
        },
    ],
)
@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "doi:",
        "doi.org/",
        "www.doi.org/",
        "dx.doi.org/",
        "http://www.doi.org/",
        "https://dx.doi.org/",
        "https://doi.org/",
    ],
)
def test_dataset_import_real_doi(runner, project, doi, prefix, sleep_after):
    """Test dataset import for existing DOI."""
    uri = prefix + doi["doi"]
    result = runner.invoke(cli, ["dataset", "import", uri], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "ls", "-c", "slug,creators"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert doi["slug"] in result.output
    assert doi["creator"] in result.output

    result = runner.invoke(cli, ["dataset", "ls-tags", doi["slug"]])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert doi["version"] in result.output

    dataset = get_dataset_with_injection(doi["slug"])
    assert doi["doi"] in dataset.same_as.url
    assert dataset.date_created is None
    assert dataset.date_published is not None
    assert doi["keywords"] == set(dataset.keywords)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize(
    "doi, input",
    [
        ("10.5281/zenodo.3239980", "n"),
        ("10.5281/zenodo.3188334", "y"),
        ("10.7910/DVN/TJCLKP", "n"),
        ("10.7910/DVN/F4NUMR", "y"),
        ("10.5281/zenodo.3236928", "n"),
        ("10.5281/zenodo.2671633", "n"),
        ("10.5281/zenodo.3237420", "n"),
        ("10.5281/zenodo.2669459", "n"),
        ("10.5281/zenodo.2371189", "n"),
        ("10.5281/zenodo.2651343", "n"),
        ("10.5281/zenodo.1467859", "n"),
        ("10.5281/zenodo.3240078", "n"),
        ("10.5281/zenodo.3240053", "n"),
        ("10.5281/zenodo.3240010", "n"),
        ("10.5281/zenodo.3240012", "n"),
        ("10.5281/zenodo.3240006", "n"),
        ("10.5281/zenodo.3239996", "n"),
        ("10.5281/zenodo.3239256", "n"),
        ("10.5281/zenodo.3237813", "n"),
        ("10.5281/zenodo.3239988", "n"),
        ("10.5281/zenodo.1175627", "n"),
        ("10.5281/zenodo.3490468", "n"),
    ],
)
@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_real_param(doi, input, runner, project, sleep_after):
    """Test dataset import and check metadata parsing."""
    result = runner.invoke(cli, ["dataset", "import", "--slug", "remote", doi], input=input)

    if "y" == input:
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert "OK" in result.output
        dataset = get_dataset_with_injection("remote")
        assert doi in dataset.same_as.url
    else:
        assert 1 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize(
    "doi", [("10.5281/zenodo.3239984", "n"), ("zenodo.org/record/3239986", "n"), ("10.5281/zenodo.3239982", "n")]
)
@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_uri_404(doi, runner, project, sleep_after):
    """Test dataset import and check that correct exception is raised."""
    result = runner.invoke(cli, ["dataset", "import", doi[0]], input=doi[1])
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_real_doi_warnings(runner, project, sleep_after):
    """Test dataset import for existing DOI and dataset."""
    result = runner.invoke(cli, ["dataset", "import", "10.5281/zenodo.1438326"], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Warning: Newer version found" in result.output
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "import", "10.5281/zenodo.1438326"], input="y")
    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Warning: Newer version found" in result.output
    assert "Error: Dataset exists:" in result.output

    result = runner.invoke(cli, ["dataset", "import", "10.5281/zenodo.597964"], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Warning: Newer version found" not in result.output
    assert "Error: Dataset exists:" not in result.output
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "pyndl_naive_discr_v1.1.2" in result.output


@pytest.mark.parametrize(
    "doi,err",
    [
        ("10.5281/zenodo.5979642342", "record not found"),
        ("10.7910/DVN/S8MSVFXXXX", "Provider not found: DVN"),
        ("10.1371/journal.pgen.1001111", "Provider not found: journal"),
        ("10.5281/zenodo.4557383", "no files have been found"),  # A restricted dataset
        ("https://zenodo.org/record/2621201248", "record not found"),
        ("https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/F4NUMRXXXX", "record not found"),
    ],
)
@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_expected_err(runner, project, doi, err):
    """Test error raising for invalid DOI."""
    result = runner.invoke(cli, ["dataset", "import", doi], input="y")
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert err in result.output


@pytest.mark.parametrize(
    "url",
    [
        "https://zenodo.org/record/2621208",
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/F4NUMR",
        "https://zenodo.org/record/830044",
    ],
)
@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_real_http(runner, project, url, sleep_after):
    """Test dataset import through HTTPS."""
    result = runner.invoke(cli, ["dataset", "import", "-y", url], input="n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Do you wish to download this version?" not in result.output
    assert "OK" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_and_extract(runner, project, sleep_after):
    """Test dataset import and extract files."""
    url = "https://zenodo.org/record/2658634"
    result = runner.invoke(cli, ["dataset", "import", "--extract", "--slug", "remote", url], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("remote")
    extracted_file = "data/remote/quantling-pyndl-c34259c/doc/make.bat"
    assert dataset.find_file(extracted_file)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_different_names(runner, project, sleep_after):
    """Test can import same DOI under different names."""
    doi = "10.5281/zenodo.2658634"
    result = runner.invoke(cli, ["dataset", "import", "--slug", "name-1", doi], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "import", "--slug", "name-2", doi], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_ignore_uncompressed_files(runner, project, sleep_after):
    """Test dataset import ignores uncompressed files."""
    url = "https://zenodo.org/record/3251128"
    result = runner.invoke(cli, ["dataset", "import", "--extract", url], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Gorne_Diaz_database_2019.csv" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_reimport_removed_dataset(runner, project, sleep_after):
    """Test re-importing of deleted datasets works."""
    doi = "10.5281/zenodo.2658634"
    result = runner.invoke(cli, ["dataset", "import", doi, "--slug", "my-dataset"], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "rm", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "import", doi, "--slug", "my-dataset"], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_preserve_names(runner, project, sleep_after):
    """Test import keeps original file names."""
    doi = "10.7910/DVN/F4NUMR"
    result = runner.invoke(cli, ["dataset", "import", doi], input="y")
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "ls-files"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Data Key 2002-2006" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "uri",
    [
        "https://dev.renku.ch/datasets/860f6b5b46364c83b6a9b38ef198bcc0",
        "https://dev.renku.ch/datasets/860f6b5b46364c83b6a9b38ef198bcc0/",
        "https://dev.renku.ch/projects/renku-test-projects/dataset-import/datasets/860f6b5b46364c83b6a9b38ef198bcc0",
        "https://dev.renku.ch/projects/renku-test-projects/dataset-import/datasets/remote-dataset/",
    ],
)
def test_dataset_import_renku_provider(runner, project, uri):
    """Test dataset import from Renku datasets."""
    result = runner.invoke(cli, ["dataset", "import", "--name", "my-dataset", uri], input="y")

    assert 0 == result.exit_code, format_result_exception(result)
    assert "d66d5e766eeaa69bee06b96e061e3bfc23978b11" in result.output
    assert "business-employment-data-december-2020-quarter-csv.zip" in result.output
    assert "OK" in result.output

    dataset = get_dataset_with_injection("my-dataset")
    assert "business-employment-data-december-2020-quarter-csv.zip" in [Path(f.entity.path).name for f in dataset.files]

    # NOTE: Check that schema:sameAs is always set to canonical dataset URI regardless of import URI
    canonical_uri = "https://dev.renku.ch/datasets/860f6b5b46364c83b6a9b38ef198bcc0"
    assert dataset.same_as.url["@id"] == canonical_uri

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "uri",
    [
        "https://dev.renku.ch/projects/renku-test-projects/test-subgroup-1/dataset-import-in-sub-groups/datasets/"
        "b13b729330684b869741760c7c5b5e83",
        "https://dev.renku.ch/projects/renku-test-projects/test-subgroup-1/dataset-import-in-sub-groups/datasets/"
        "remote-dataset",
    ],
)
def test_dataset_import_renku_provider_with_subgroups(runner, project, uri):
    """Test dataset import from Renku datasets in projects within subgroups."""
    result = runner.invoke(cli, ["dataset", "import", "--name", "my-dataset", uri], input="y")

    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("my-dataset")

    assert "business-employment-data-december-2020-quarter-csv.zip" in [Path(f.entity.path).name for f in dataset.files]

    # NOTE: Check that schema:sameAs is always set to canonical dataset URI regardless of import URI
    canonical_uri = "https://dev.renku.ch/datasets/b13b729330684b869741760c7c5b5e83"
    assert dataset.same_as.url["@id"] == canonical_uri
    assert dataset.derived_from is None


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_import_renkulab_dataset_with_image(runner, project, with_injection):
    """Test dataset import from Renkulab projects."""
    result = runner.invoke(
        cli, ["dataset", "import", "https://dev.renku.ch/datasets/4f36f891bb7c4b2bab137633cc270a40"], input="y"
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" in result.output

    assert "0" in result.output
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "ls-files"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "bla" in result.output

    with with_injection():
        dataset = [d for d in DatasetGateway().get_all_active_datasets()][0]
    assert 2 == len(dataset.images)
    img1 = next(i for i in dataset.images if i.position == 1)
    img2 = next(i for i in dataset.images if i.position == 2)

    assert img1.content_url == "https://example.com/image1.jpg"
    assert img2.content_url.endswith("/2.png")
    assert os.path.exists(project.path / img2.content_url)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "datadir_option,expected_datadir", [([], Path(DATA_DIR) / "remote"), (["--datadir", "mydir"], Path("mydir"))]
)
def test_import_renku_dataset_preserves_directory_hierarchy(runner, project, datadir_option, expected_datadir):
    """Test dataset imported from Renku projects have correct directory hierarchy."""
    url = "https://dev.renku.ch/datasets/1a637fd1a7a64d1fb9aa157e7033cd1c"
    assert 0 == runner.invoke(cli, ["dataset", "import", "--yes", "--name", "remote", url] + datadir_option).exit_code

    dataset = get_dataset_with_injection("remote")
    paths = ["README.md", os.path.join("python", "data", "README.md"), os.path.join("r", "data", "README.md")]

    data_dir = Path(dataset.get_datadir())
    assert data_dir == expected_datadir
    for path in paths:
        assert (data_dir / path).exists()
        file = dataset.find_file(data_dir / path)
        assert file.based_on
        assert file.based_on.path.endswith(path)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize("url", ["https://dev.renku.ch/datasets/e3e1beba05594fdd8e4682963cec9fe2"])
def test_dataset_import_renku_fail(runner, project, monkeypatch, url):
    """Test dataset import fails if cannot clone repo."""
    from renku.core.dataset.providers import renku

    def clone_renku_repository_mock(*_, **__):
        raise errors.GitError

    with monkeypatch.context() as monkey:
        monkey.setattr(renku, "clone_renku_repository", clone_renku_repository_mock)

        result = runner.invoke(cli, ["dataset", "import", url], input="y")
        assert 2 == result.exit_code, format_result_exception(result)
        assert f"Invalid parameter value for {url}" in result.output
        assert "Cannot clone remote projects:" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize("url", ["https://dev.renku.ch/datasets/e3e1beba-0559-4fdd-8e46-82963cec9fe2"])
def test_dataset_import_renku_missing_project(runner, project, missing_kg_project_responses, url):
    """Test dataset import fails when cannot find project in KG."""
    result = runner.invoke(cli, ["dataset", "import", url], input="y")
    assert 1 == result.exit_code
    assert "Resource not found in knowledge graph" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "url,exit_code",
    [
        ("https://dev.renku.ch/projects/renku-testing/project-9/", 2),
        ("https://dev.renku.ch/projects/renku-testing/project-9/datasets/b9f7b21b-8b00-42a2-976a-invalid", 1),
        ("https://dev.renku.ch/datasets/10.5281%2Fzenodo.666", 2),
    ],
)
def test_dataset_import_renkulab_errors(runner, project, url, exit_code):
    """Test usage errors in Renku dataset import."""
    assert exit_code == runner.invoke(cli, ["dataset", "import", url], input="y").exit_code

    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "uri, message",
    [
        (
            "https://dev.renku.ch/projects/renku-test-projects/dataset-import/datasets/non-existing-dataset",
            "Cannot fetch dataset with slug 'non-existing-dataset'",
        ),
        (
            "https://dev.renku.ch/projects/invalid/project-path/datasets/860f6b5b46364c83b6a9b38ef198bcc0",
            "Cannot find project in the knowledge graph:",
        ),
    ],
)
def test_dataset_import_renku_provider_errors(runner, project, uri, message):
    """Test errors in Renku dataset import due to invalid project or dataset name."""
    result = runner.invoke(cli, ["dataset", "import", uri])

    assert 1 == result.exit_code
    assert message in result.output

    assert 0 == runner.invoke(cli, ["dataset", "ls"]).exit_code


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize("url", ["https://dev.renku.ch/datasets/e3e1beba05594fdd8e4682963cec9fe2"])
def test_dataset_reimport_renkulab_dataset(runner, project, url):
    """Test dataset import for existing dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "import", url], input="y").exit_code

    result = runner.invoke(cli, ["dataset", "import", url], input="y")
    assert 1 == result.exit_code, format_result_exception(result)
    assert "Dataset exists" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_renku_dataset_import_missing_lfs_objects(runner, project):
    """Test importing a dataset with missing LFS objects fails."""
    result = runner.invoke(
        cli, ["dataset", "import", "--yes", "https://dev.renku.ch/datasets/5c11e3212bea458c94ceabccf4257a54"]
    )

    assert 1 == result.exit_code
    assert "Error: Cannot pull LFS objects from server" in result.output
    assert "[404] Object does not exist on the server or you don't have permissions to access it" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "provider,params,output,input",
    [
        ("zenodo", [], "zenodo.org/deposit", None),
        ("dataverse", ["--dataverse-name", "sdsc-test-dataverse"], "doi:", "1"),
        # ("olos",["--dlcm-server", "https://sandbox.dlcm.ch/"], "sandbox.dlcm.ch/ingestion/preingest/deposits/", None),
    ],
)
def test_dataset_export_upload_file(
    runner,
    tmpdir,
    project,
    zenodo_sandbox,
    dataverse_demo,
    olos_sandbox,
    provider,
    params,
    output,
    input,
    with_injection,
):
    """Test successful uploading of a file to Zenodo/Dataverse deposit."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with with_injection():
        with with_dataset(name="my-dataset", commit_database=True) as dataset:
            dataset.description = "awesome dataset"
            dataset.creators[0].affiliation = "eth"

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    result = runner.invoke(
        cli, ["dataset", "export", "my-dataset", provider] + params, input=input, catch_exceptions=False
    )

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Exported to:" in result.output
    assert output in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "provider,params,output,input",
    [
        ("zenodo", [], "zenodo.org/deposit", None),
        ("dataverse", ["--dataverse-name", "sdsc-test-dataverse"], "doi:", "1"),
        # ("olos",["--dlcm-server", "https://sandbox.dlcm.ch/"], "sandbox.dlcm.ch/ingestion/preingest/deposits/", None),
    ],
)
def test_dataset_export_upload_tag(
    runner,
    tmpdir,
    project,
    zenodo_sandbox,
    dataverse_demo,
    olos_sandbox,
    provider,
    params,
    output,
    input,
    with_injection,
):
    """Test successful uploading of a file to Zenodo/Dataverse deposit."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with with_injection():
        with with_dataset(name="my-dataset", commit_database=True) as dataset:
            dataset.description = "awesome dataset"
            dataset.creators[0].affiliation = "eth"

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # create data file
    new_file = tmpdir.join("datafile2.csv")
    new_file.write("1,2,3,4")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "2.0"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider, "-t", "2.0"] + params, input=input)

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Exported to:" in result.output
    assert output in result.output

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider, "-t", "1.0"] + params, input=input)

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Exported to:" in result.output
    assert output in result.output

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider] + params, input=f"0\n{input}")  # HEAD

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Exported to:" in result.output
    assert output in result.output


@pytest.mark.integration
def test_dataset_export_to_local(runner, tmp_path):
    """Test exporting a version of dataset to a local directory."""
    url = "https://gitlab.dev.renku.ch/renku-python-integration-tests/lego-datasets.git"
    repository = Repository.clone_from(url=url, path=tmp_path / "repo")
    # NOTE: Install LFS and disable LFS smudge filter to make sure that we can get valid content in that case
    repository.lfs.install(skip_smudge=True)

    os.chdir(repository.path)

    result = runner.invoke(cli, ["migrate"])

    assert 0 == result.exit_code, format_result_exception(result)

    output_path: Path = tmp_path / "exported"

    result = runner.invoke(cli, ["dataset", "export", "parts", "local", "-t", "v1", "-p", output_path])

    assert 0 == result.exit_code, format_result_exception(result)
    assert f"Dataset metadata was copied to {output_path}/METADATA.yml" in result.output
    assert f"Exported to: {output_path}" in result.output
    assert {"METADATA.yml", "README.md", "part_categories.csv", "part_relationships.csv", "parts.csv"} == {
        str(f.relative_to(output_path)) for f in get_files(output_path)
    }
    assert (output_path / "parts.csv").read_text().startswith("part_num,name,part_cat_id,part_material")
    assert (output_path / "part_relationships.csv").read_text().startswith("rel_type,child_part_num,parent_part_num")
    assert (output_path / "part_categories.csv").read_text().startswith("id,name")
    assert (output_path / "README.md").read_text().startswith("First version updated on 27.02.2022")
    assert "- '@id': /dataset-files/" in (output_path / "METADATA.yml").read_text()

    # NOTE: Export fails if destination directory is not empty
    result = runner.invoke(cli, ["dataset", "export", "parts", "local", "-t", "v2", "--path", output_path])

    assert 1 == result.exit_code, format_result_exception(result)
    assert f"Destination directory is not empty: '{output_path}'" in result.output

    # NOTE: Export creates a default directory inside the project is no output path is set
    result = runner.invoke(cli, ["dataset", "export", "parts", "local"], input="2\n")  # v1

    assert 0 == result.exit_code, format_result_exception(result)
    assert f"Dataset metadata was copied to {repository.path}/data/parts-v1/METADATA.yml" in result.output
    assert f"Exported to: {repository.path}/data/parts-v1" in result.output
    assert repository.is_dirty()
    assert (repository.path / "data" / "parts-v1" / "part_relationships.csv").exists()
    assert (repository.path / "data" / "parts-v1" / "parts.csv").read_text() == (
        repository.path / "data" / "parts-v1" / "parts.csv"
    ).read_text()


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "provider,params,output,input",
    [
        ("zenodo", [], "zenodo.org/deposit", None),
        ("dataverse", ["--dataverse-name", "sdsc-test-dataverse"], "doi:", "1"),
        # ("olos",["--dlcm-server", "https://sandbox.dlcm.ch/"], "sandbox.dlcm.ch/ingestion/preingest/deposits/", None),
    ],
)
def test_dataset_export_upload_multiple(
    runner,
    tmpdir,
    project,
    zenodo_sandbox,
    dataverse_demo,
    olos_sandbox,
    provider,
    params,
    output,
    input,
    with_injection,
):
    """Test successful uploading of a files to Zenodo deposit."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    paths = []
    for i in range(3):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with with_injection():
        with with_dataset(name="my-dataset", commit_database=True) as dataset:
            dataset.description = "awesome dataset"
            dataset.creators[0].affiliation = "eth"

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider] + params, input=input)

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Exported to:" in result.output
    assert output in result.output


@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
def test_dataset_export_upload_failure(runner, tmpdir, project, zenodo_sandbox):
    """Test failed uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", "zenodo"])

    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "metadata.creators.0.affiliation" in result.output
    assert "metadata.description" in result.output


@pytest.mark.publish
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "provider,params,output",
    [("zenodo", [], "zenodo.org/record"), ("dataverse", ["--dataverse-name", "sdsc-published-test-dataverse"], "doi:")],
)
def test_dataset_export_published_url(
    runner, tmpdir, project, zenodo_sandbox, dataverse_demo, with_injection, provider, params, output
):
    """Test publishing of dataset."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset", "-k", "keyword", "-k", "data"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with with_injection(), with_dataset(name="my-dataset", commit_database=True) as dataset:
        dataset.description = "awesome dataset"
        dataset.creators[0].affiliation = "eth"

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider, "--publish"] + params)

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Exported to:" in result.output
    assert output in result.output

    m = re.search(r"Exported to:\s*(\S*)$", result.output, flags=re.MULTILINE)
    doi = m.group(1)
    result = runner.invoke(cli, ["dataset", "import", doi, "--name", "imported"], input="y")
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("imported")
    assert {"data", "keyword"} == set(dataset.keywords)


@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
def test_export_dataset_wrong_provider(runner, project, tmpdir):
    """Test non-existing provider."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", "unsupported-provider"])
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Invalid value" in result.output


@pytest.mark.integration
@pytest.mark.parametrize("provider", ["zenodo", "dataverse", "local", "olos"])
def test_dataset_export_non_existing(runner, project, provider):
    """Check dataset not found exception raised."""
    result = runner.invoke(cli, ["dataset", "export", "non-existing", provider])

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Dataset 'non-existing' is not found." in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "provider,params",
    [
        ("zenodo", []),
        ("dataverse", ["--dataverse-name", "sdsc-test-dataverse"]),
        ("olos", ["--dlcm-server", "https://sandbox.dlcm.ch/"]),
    ],
)
def test_export_dataset_unauthorized(
    runner, project, tmpdir, zenodo_sandbox, dataverse_demo, olos_sandbox, provider, params
):
    """Test unauthorized exception raised."""
    set_value(provider, "access_token", "not-a-token")
    project.repository.add(".renku/renku.ini")
    project.repository.commit("update renku.ini")

    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", provider] + params)

    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Access unauthorized - update access token." in result.output, format_result_exception(result)

    secret = get_value("zenodo", "secret")
    assert secret is None


@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
def test_export_dataverse_no_dataverse_name(runner, project, dataverse_demo):
    """Test export without providing a dataverse name."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "export", "my-dataset", "dataverse"])

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Dataverse name is required." in result.output


@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
def test_export_dataverse_no_dataverse_url(runner, project, dataverse_demo):
    """Test export without providing a dataverse server url."""
    remove_value("dataverse", "server_url")
    project.repository.add(".renku/renku.ini")
    project.repository.commit("renku.ini")

    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ["dataset", "export", "my-dataset", "dataverse", "--dataverse-name", "sdsc-test-dataverse"]
    )

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Dataverse server URL is required." in result.output


@pytest.mark.skip(reason="Dataset doesn't exist yet at the tagged commit. Fix in #/renku-python/issues/2210")
@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
def test_export_imported_dataset_to_dataverse(runner, project, dataverse_demo, zenodo_sandbox):
    """Test exporting an imported Zenodo dataset to dataverse."""
    result = runner.invoke(cli, ["dataset", "import", "10.5281/zenodo.2658634", "--slug", "my-data"], input="y")
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli, ["dataset", "export", "my-data", "dataverse", "--dataverse-name", "sdsc-test-dataverse"], input="2"
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output


@pytest.mark.integration
@pytest.mark.vcr
def test_add_from_url_to_destination(runner, project):
    """Test add data from a URL to a new destination."""
    url = "https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/docs/Makefile"
    assert 0 == runner.invoke(cli, ["dataset", "create", "remote"], catch_exceptions=False).exit_code

    result = runner.invoke(cli, ["dataset", "add", "--copy", "remote", "-d", "new-name", url])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    relative_path = os.path.join("data", "remote", "new-name")
    assert (project.path / relative_path).exists()
    assert (project.path / relative_path).is_file()

    dataset = get_dataset_with_injection("remote")
    assert dataset.find_file(relative_path) is not None


@pytest.mark.integration
@pytest.mark.parametrize(
    "params,path",
    [
        # add data with no destination
        (["-s", "docker"], "data/remote/docker/r/Dockerfile"),
        (["-s", "docker/r/Dockerfile"], "data/remote/Dockerfile"),
        # add data to a non-existing destination
        (["-s", "docker", "-d", "new"], "data/remote/new/r/Dockerfile"),
        (["-s", "docker/r", "-d", "new"], "data/remote/new/Dockerfile"),
        (["-s", "docker/r/Dockerfile", "-d", "path/with/sub-dirs/new"], "data/remote/path/with/sub-dirs/new"),
    ],
)
@pytest.mark.vcr
def test_add_from_git_to_new_path(runner, project, params, path):
    """Test add data from a git repository with no destination or to a non-existing destination."""
    remote = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"
    assert 0 == runner.invoke(cli, ["dataset", "create", "remote"], catch_exceptions=False).exit_code

    result = runner.invoke(cli, ["dataset", "add", "--copy", "remote", "--ref", "0.3.0", remote] + params)

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert Path(path).exists()

    file = get_dataset_with_injection("remote").find_file(path)
    assert file.source == remote
    assert file.based_on.url == remote

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.parametrize(
    "params,path",
    [
        (["-s", "docker", "-d", "existing"], "data/remote/existing/docker/r/Dockerfile"),
        (["-s", "docker/r", "-d", "existing"], "data/remote/existing/r/Dockerfile"),
        (["-s", "docker/r/Dockerfile", "-d", "existing"], "data/remote/existing/Dockerfile"),
    ],
)
@pytest.mark.vcr
def test_add_from_git_to_existing_path(runner, project, params, path, no_datadir_commit_warning):
    """Test add data to datasets from a git repository to an existing path."""
    remote = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"
    assert 0 == runner.invoke(cli, ["dataset", "create", "remote"], catch_exceptions=False).exit_code

    write_and_commit_file(project.repository, project.path / "data" / "remote" / "existing" / ".gitkeep", "")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "remote", "--ref", "0.3.0", remote] + params)

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert Path(path).exists()

    file = get_dataset_with_injection("remote").find_file(path)
    assert file.source == remote
    assert file.based_on.url == remote

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.parametrize(
    "params,files",
    [
        (["-s", "docker*"], {"py3.7", "cuda10.0-tf1.14", "cuda9.2", "r3.6.1", "bioc3_10"}),
        (["-s", "docker/*"], {"py3.7", "cuda10.0-tf1.14", "cuda9.2", "r3.6.1", "bioc3_10"}),
        (["-s", "docker/**"], {"py3.7", "cuda10.0-tf1.14", "cuda9.2", "r3.6.1", "bioc3_10"}),
        (["-s", "docker/*/*sh"], {"entrypoint.sh", "fix-permissions.sh"}),
        (["-s", "docker/*/fix*sh"], {"fix-permissions.sh"}),  # Multiple files copied to the same destination
        (["-s", "*/**/*sh"], {"entrypoint.sh", "fix-permissions.sh"}),
    ],
)
@pytest.mark.vcr
def test_add_from_git_with_wildcards_to_new_path(runner, project, params, files):
    """Test add data from a git repository using wildcards to a non-existing destination."""
    remote = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    result = runner.invoke(
        cli, ["dataset", "add", "remote", "--copy", "--create", "--ref", "0.5.2", "-d", "new", remote] + params
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert files == set(os.listdir("data/remote/new"))


@pytest.mark.integration
@pytest.mark.parametrize(
    "params,files",
    [
        (["-s", "docker*"], {"docker"}),
        (["-s", "docker/*"], {"py3.7", "cuda10.0-tf1.14", "cuda9.2", "r3.6.1", "bioc3_10"}),
        (["-s", "docker/**"], {"py3.7", "cuda10.0-tf1.14", "cuda9.2", "r3.6.1", "bioc3_10"}),
        (["-s", "docker/*/*sh"], {"entrypoint.sh", "fix-permissions.sh"}),
        (["-s", "docker/*/fix*sh"], {"fix-permissions.sh"}),
        (["-s", "*/**/*sh"], {"entrypoint.sh", "fix-permissions.sh"}),
    ],
)
@pytest.mark.vcr
def test_add_from_git_with_wildcards_to_existing_path(runner, project, params, files):
    """Test add data from a git repository using wildcards with no destination or to a non-existing destination."""
    remote = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    result = runner.invoke(
        cli,
        ["dataset", "add", "remote", "--copy", "--create", "--ref", "0.5.2", remote] + params,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert files == set(os.listdir("data/remote"))


@pytest.mark.integration
@retry_failed
def test_add_data_in_multiple_places_from_git(runner, project):
    """Test add same data to datasets in multiple places from a git repository."""
    url = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    assert 0 == runner.invoke(cli, ["dataset", "create", "remote"]).exit_code

    args = ["dataset", "add", "--copy", "remote", "--ref", "0.3.0"]
    assert 0 == runner.invoke(cli, args + ["-s", "docker/base/Dockerfile", url]).exit_code

    dataset = get_dataset_with_injection("remote")
    data_dir = Path(dataset.get_datadir())
    based_on_id = dataset.find_file(data_dir / "Dockerfile").based_on.id

    assert 0 == runner.invoke(cli, args + ["-s", "docker", url]).exit_code

    dataset = get_dataset_with_injection("remote")
    assert based_on_id == dataset.find_file(data_dir / "Dockerfile").based_on.id
    assert based_on_id == dataset.find_file(data_dir / "docker" / "base" / "Dockerfile").based_on.id


@pytest.mark.integration
@pytest.mark.parametrize(
    "params,n_urls,message",
    [
        ([], 0, "No URL is specified"),
        (["-s", "file", "-d", "new-file"], 0, "No URL is specified"),
        (["-s", "file"], 2, "Cannot use '--source' with multiple URLs."),
        (["-s", "non-existing"], 1, "No such file or directory"),
        (["-s", "docker/*Dockerfile"], 1, "No such file or directory"),
        (["-s", "docker", "-d", "LICENSE"], 1, "Destination is not a directory"),
        (["-s", "LICENSE", "-s", "Makefile", "-d", "LICENSE"], 1, "Destination is not a directory"),
        (["-d", "LICENSE"], 1, "Destination is not a directory"),
    ],
)
@retry_failed
@pytest.mark.vcr
def test_usage_error_in_add_from_git(runner, project, params, n_urls, message):
    """Test user's errors when adding to a dataset from a git repository."""
    remote = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    # create a dataset and add a file to it
    result = runner.invoke(
        cli,
        ["dataset", "add", "--copy", "remote", "--create", "--ref", "0.3.0", "-s", "LICENSE", remote],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    urls = n_urls * [remote]

    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "remote", "--ref", "0.3.0"] + params + urls, catch_exceptions=False
    )
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert message in result.output


@pytest.mark.integration
@pytest.mark.parametrize("params", [["--all"], ["-I", "README.md"], ["-I", "R*"], ["remote"]])
@retry_failed
@pytest.mark.vcr
def test_dataset_update(project, runner, params):
    """Test local copy is updated when remote file is updates."""
    url = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    # Add dataset to project
    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "remote", "--ref", "0.3.0", "-s", "README.md", url]
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    before = get_dataset_with_injection("remote").find_file("data/remote/README.md")

    assert 0 == runner.invoke(cli, ["dataset", "update"] + params, catch_exceptions=False).exit_code

    after = get_dataset_with_injection("remote").find_file("data/remote/README.md")

    assert after.id != before.id
    assert after.date_added != before.date_added
    assert after.source == before.source
    assert after.based_on.id != before.based_on.id
    assert after.based_on.path == before.based_on.path
    assert after.based_on.url == url
    assert after.based_on.checksum in after.based_on.id

    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.parametrize("doi", ["10.5281/zenodo.2658634"])
@retry_failed
@pytest.mark.vcr
def test_dataset_update_zenodo(project, runner, doi):
    """Test updating datasets from external providers."""
    result = runner.invoke(
        cli, ["dataset", "import", "--slug", "imported_dataset", doi], input="y", catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    commit_sha_after_file1_delete = project.repository.head.commit.hexsha

    before_dataset = get_dataset_with_injection("imported_dataset")

    result = runner.invoke(cli, ["dataset", "update", "--all", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "The following imported datasets will be updated" in result.output
    assert "imported_dataset" in result.output
    assert commit_sha_after_file1_delete == project.repository.head.commit.hexsha
    assert not project.repository.is_dirty()

    result = runner.invoke(cli, ["dataset", "update", "imported_dataset"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    after_dataset = get_dataset_with_injection("imported_dataset")
    assert after_dataset.version != before_dataset.version
    assert after_dataset.id != before_dataset.id
    assert after_dataset.derived_from is None
    assert after_dataset.same_as is not None
    assert after_dataset.same_as != before_dataset.same_as
    assert after_dataset.date_created is None
    assert after_dataset.date_published is not None


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.parametrize("doi", ["10.7910/DVN/F4NUMR"])
@retry_failed
@pytest.mark.vcr
def test_dataset_update_dataverse(project, runner, doi, with_injection):
    """Test updating datasets from external providers.

    Since dataverse does not have DOIs/IDs for each version, we need to fake the check.
    """
    result = runner.invoke(
        cli, ["dataset", "import", "--slug", "imported_dataset", doi], input="y", catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    assert 0 == runner.invoke(cli, ["dataset", "rm-tags", "imported_dataset", "2.2"], catch_exceptions=False).exit_code

    with with_injection():
        with with_dataset(name="imported_dataset", commit_database=True) as dataset:
            dataset.version = "0.1"

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    before_dataset = get_dataset_with_injection("imported_dataset")

    result = runner.invoke(cli, ["dataset", "update", "imported_dataset"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    after_dataset = get_dataset_with_injection("imported_dataset")
    assert after_dataset.version != before_dataset.version
    assert after_dataset.id != before_dataset.id
    assert after_dataset.derived_from is None
    assert after_dataset.same_as is not None


@pytest.mark.skip(
    "DatasetProvenance creates a derived dataset due to some problem, we should investigate in a followup issue"
)
@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_update_renku(project, runner, with_injection):
    """Test updating datasets from renku provider."""
    uri = "https://dev.renku.ch/datasets/860f6b5b46364c83b6a9b38ef198bcc0"
    assert 0 == runner.invoke(cli, ["dataset", "import", "--name", "remote-dataset", uri], input="y").exit_code

    with with_injection():
        with with_dataset(name="remote-dataset", commit_database=True) as dataset:
            # NOTE: To mock an update we schema:sameAs to a dataset that has an update
            update_uri = "https://dev.renku.ch/datasets/04b463b01b514833b236186a941f6259"
            dataset.same_as = Url(url_id=update_uri)

    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    commit_sha_after_file1_delete = project.repository.head.commit.hexsha

    before_dataset = get_dataset_with_injection("imported_dataset")

    result = runner.invoke(cli, ["dataset", "update", "--all", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "The following imported datasets will be updated" in result.output
    assert "remote-dataset" in result.output
    assert commit_sha_after_file1_delete == project.repository.head.commit.hexsha
    assert not project.repository.is_dirty()

    result = runner.invoke(cli, ["dataset", "update", "--all"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    after_dataset = get_dataset_with_injection("remote-dataset")
    assert after_dataset.id != before_dataset.id
    assert after_dataset.derived_from is None
    latest_uri = "https://dev.renku.ch/datasets/e55070d995b34b9ba319c6e66f883f00"
    assert latest_uri == after_dataset.same_as.url["@id"]

    before_filenames = [Path(f.entity.path).name for f in before_dataset.files]
    assert "electronic-card-transactions-february-2021-csv.zip" not in before_filenames
    after_filenames = [Path(f.entity.path).name for f in after_dataset.files]
    assert "electronic-card-transactions-february-2021-csv.zip" in after_filenames


@pytest.mark.integration
@retry_failed
def test_dataset_update_remove_file(project, runner):
    """Test local copy is removed when remote file is removed."""
    # Add dataset to project
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "remote",
            "-s",
            "docs/authors.rst",
            "--ref",
            "v0.3.0",
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    file_path = project.path / DATA_DIR / "remote" / "authors.rst"
    assert file_path.exists()

    # docs/authors.rst does not exists in v0.5.0

    result = runner.invoke(cli, ["dataset", "update", "--ref", "v0.5.0", "--all"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Some files are deleted:" in result.output
    assert file_path.exists()

    result = runner.invoke(cli, ["dataset", "update", "--ref", "v0.5.0", "--delete", "--all"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert not file_path.exists()


@pytest.mark.integration
@pytest.mark.parametrize("params", [["-I", "non-existing"], ["non-existing-dataset"]])
@retry_failed
@pytest.mark.vcr
def test_dataset_invalid_update(project, runner, params):
    """Test updating a non-existing path."""
    # Add dataset to project
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "remote",
            "-s",
            "README.md",
            "--ref",
            "0.3.0",
            "https://github.com/SwissDataScienceCenter/renku-jupyter.git",
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "update"] + params, catch_exceptions=False)
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.integration
@pytest.mark.parametrize("params", [["--all"], ["-I", "CHANGES.rst"], ["-I", "CH*"], ["dataset-1", "dataset-2"]])
@retry_failed
@pytest.mark.vcr
def test_dataset_update_multiple_datasets(project, runner, params):
    """Test update with multiple datasets."""
    path1 = project.path / DATA_DIR / "dataset-1" / "CHANGES.rst"
    path2 = project.path / DATA_DIR / "dataset-2" / "CHANGES.rst"
    # Add dataset to project
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "dataset-1",
            "--ref",
            "v0.3.0",
            "-s",
            "CHANGES.rst",
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "dataset-2",
            "--ref",
            "v0.3.0",
            "-s",
            "CHANGES.rst",
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    assert "v0.4.0" not in path1.read_text()
    assert "v0.4.0" not in path2.read_text()

    result = runner.invoke(cli, ["dataset", "update"] + params, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    assert "v0.4.0" in path1.read_text()
    assert "v0.4.0" in path2.read_text()


@pytest.mark.integration
@retry_failed
def test_empty_update(project, runner):
    """Test update when nothing changed does not create a commit."""
    # Add dataset to project
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "remote",
            "--ref",
            "0.3.0",
            "-s",
            "README.md",
            "https://github.com/SwissDataScienceCenter/renku-jupyter.git",
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    commit_sha_before = project.repository.head.commit.hexsha
    result = runner.invoke(cli, ["dataset", "update", "--ref", "0.3.0", "--all"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_after == commit_sha_before


@pytest.mark.integration
@retry_failed
def test_import_from_renku_project(tmpdir, project, runner):
    """Check metadata for an imported dataset from other renkulab repo."""
    from renku.domain_model.project_context import project_context

    url = "https://gitlab.dev.renku.ch/renku-testing/project-9.git"

    def get_remote_file():
        repo_path = tmpdir.mkdir("remote_repo")
        os.environ["GIT_LFS_SKIP_SMUDGE"] = "1"
        Repository.clone_from(url=url, path=repo_path, recursive=True)

        with project_context.with_path(repo_path):
            with chdir(repo_path):
                runner.invoke(cli, ["migrate", "--strict"])

            dataset = get_dataset_with_injection("testing-create-04")
            return dataset.find_file("data/testing-create-04/ie_data_with_TRCAPE.xls")

    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--create",
            "remote-dataset",
            "-s",
            "data/testing-create-04/ie_data_with_TRCAPE.xls",
            "-d",
            "new-directory/ie_data_with_TRCAPE.xls",
            "--ref",
            "97f907e",
            url,
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    path = "data/remote-dataset/new-directory/ie_data_with_TRCAPE.xls"
    metadata = get_dataset_with_injection("remote-dataset").find_file(path)
    file = get_remote_file()
    assert metadata.based_on.checksum == file.entity.checksum
    assert metadata.based_on.path == file.entity.path
    assert metadata.based_on.url == url


@pytest.mark.integration
@pytest.mark.parametrize("ref", ["v0.3.0", "fe6ec65cc84bcf01e879ef38c0793208f7fab4bb"])
@retry_failed
@pytest.mark.vcr
def test_add_specific_refs(ref, runner, project):
    """Test adding a specific version of files."""
    filename = "CHANGES.rst"

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "dataset",
            "-s",
            filename,
            "--ref",
            ref,
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result)
    content = (project.path / DATA_DIR / "dataset" / filename).read_text()
    assert "v0.3.0" in content
    assert "v0.3.1" not in content


@pytest.mark.integration
@pytest.mark.parametrize("ref", ["v0.3.1", "27e29abd409c83129a3fdb8b8b0b898b23bcb229"])
@retry_failed
@pytest.mark.vcr
def test_update_specific_refs(ref, runner, project):
    """Test updating to a specific version of files."""
    filename = "CHANGES.rst"
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "dataset",
            "-s",
            filename,
            "--ref",
            "v0.3.0",
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    file = Path(DATA_DIR) / "dataset" / filename
    content = (project.path / file).read_text()
    assert "v0.3.1" not in content

    commit_sha_after_file1_delete = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "update", "--ref", ref, "--all", "--dry-run"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "The following files will be updated" in result.output
    assert str(file) in result.output
    assert commit_sha_after_file1_delete == project.repository.head.commit.hexsha
    assert not project.repository.is_dirty()

    # update data to a later version
    result = runner.invoke(cli, ["dataset", "update", "--ref", ref, "--all"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    content = (project.path / DATA_DIR / "dataset" / filename).read_text()
    assert "v0.3.1" in content
    assert "v0.3.2" not in content


@pytest.mark.integration
@retry_failed
def test_update_with_multiple_remotes_and_ref(runner, project):
    """Test updating fails when ref is ambiguous."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "dataset",
            "-s",
            "README.md",
            "https://github.com/SwissDataScienceCenter/renku-project-template.git",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # add data from another git repo
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--copy",
            "dataset",
            "-s",
            "LICENSE",
            "https://github.com/SwissDataScienceCenter/renku-notebooks.git",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # update data to a later version
    result = runner.invoke(cli, ["dataset", "update", "--ref", "any-value", "dataset"])
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Cannot specify a reference with more than one Git repository" in result.output


@pytest.mark.integration
@retry_failed
def test_files_are_tracked_in_lfs(runner, project, no_lfs_size_limit):
    """Test files added from a Git repo are tacked in Git LFS."""
    filename = "poetry.lock"
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli,
        [
            "dataset",
            "add",
            "--copy",
            "dataset",
            "-s",
            filename,
            "https://github.com/SwissDataScienceCenter/renku-python.git",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    path = f"data/dataset/{filename}"
    assert path in subprocess.check_output(["git", "lfs", "ls-files"]).decode()


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize(
    "url",
    ["https://username:password@raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/docs/Makefile"],
)
def test_add_removes_credentials(runner, project, url):
    """Check removal of credentials during adding of remote data files."""
    from urllib.parse import urlparse

    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", url])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("my-dataset")
    file = dataset.files[0]
    url_obj = urlparse(url)
    assert file.source == url_obj._replace(netloc=url_obj.hostname).geturl()


@pytest.mark.integration
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "disposition,filename",
    [
        ("Attachment; filename=Example.html", "Example.html"),
        ('INLINE; FILENAME= "an example.html"', "an example.html"),
        ("attachment; filename*= UTF-8''%e2%82%ac%20rates.csv", "€ rates.csv"),
        ("attachment;filename=\"EURO rates.csv\";filename*=utf-8''%e2%82%ac%20rates.csv", "€ rates.csv"),
    ],
)
def test_add_with_content_disposition(runner, project, monkeypatch, disposition, filename):
    """Check filename is read from content disposition."""
    import renku.core.util.requests

    url = "https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/docs/Makefile"

    with monkeypatch.context() as monkey:
        # NOTE: mock requests headers
        original_disposition = renku.core.util.requests.get_filename_from_headers

        def _fake_disposition(response):
            response.headers["content-disposition"] = disposition
            return original_disposition(response)

        monkey.setattr(renku.core.util.requests, "get_filename_from_headers", _fake_disposition)
        result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", url])
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("my-dataset")
    file = dataset.files[0]
    assert Path(file.entity.path).name == filename


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize(
    "url", ["https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/docs/Makefile"]
)
def test_check_disk_space(runner, project, monkeypatch, url):
    """Check adding to dataset prompts if disk space is not enough."""

    def disk_usage(_):
        """Mocked response."""

        class Usage(NamedTuple):
            free: int

        return Usage(free=0)

    monkeypatch.setattr(shutil, "disk_usage", disk_usage)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-data", url], catch_exceptions=False)
    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert "Insufficient disk space" in result.output

    result = runner.invoke(cli, ["dataset", "ls-files"])
    assert "index.html" not in result.output + str(result.stderr_bytes)


@pytest.mark.migration
@pytest.mark.integration
@retry_failed
def test_migration_submodule_datasets(isolated_runner, old_repository_with_submodules):
    """Test migration of datasets that use submodules."""
    from renku.domain_model.project_context import project_context

    project_path = old_repository_with_submodules.path
    os.chdir(project_path)

    assert ["remote-renku-project"] == [s.name for s in old_repository_with_submodules.repository.submodules]

    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert [] == list(old_repository_with_submodules.repository.submodules)

    with project_context.with_path(project_path):
        dataset = get_dataset_with_injection("remote")

    for file in dataset.files:
        path = Path(file.entity.path)
        assert path.exists()
        assert not path.is_symlink()
        assert file.based_on is not None
        assert Path(file.entity.path).name == Path(file.based_on.path).name
        # NOTE: Old repo URL pre-cloudnative gitlab, not changed by migration
        assert "https://dev.renku.ch/gitlab/mohammad.alisafaee/remote-renku-project.git" == file.based_on.url


@pytest.mark.integration
@pytest.mark.parametrize("url,size", [("https://www.dropbox.com/s/qcpts6fc81x6j4f/addme?dl=0", 5)])
@retry_failed
@pytest.mark.vcr
def test_dataset_add_dropbox(runner, project, url, size):
    """Test importing data from dropbox."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dropbox-data", url], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    filename = Path(parse.urlparse(url).path).name
    assert url

    datafile = project.path / "data/my-dropbox-data" / filename
    assert size == len(datafile.read_text())


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_immutability_at_import(runner, project):
    """Test first dataset's ID after import is the same as its initial identifier."""
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--name", "my-dataset", "10.7910/DVN/F4NUMR"]).exit_code

    dataset = get_dataset_with_injection("my-dataset")

    assert dataset.initial_identifier == dataset.identifier


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_immutability_after_import(runner, project):
    """Test first dataset's ID after import is the same as metadata directory."""
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--name", "my-dataset", "10.7910/DVN/F4NUMR"]).exit_code

    old_dataset = get_dataset_with_injection("my-dataset")

    # Make some modification in dataset
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-dataset", "-k", "new-data"]).exit_code

    dataset = get_dataset_with_injection("my-dataset")
    mutator = get_git_user(project.repository)
    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)


@pytest.mark.integration
@retry_failed
def test_immutability_after_update(project, runner):
    """Test dataset is mutated after an update."""
    url = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    result = runner.invoke(cli, ["dataset", "add", "--create", "my-data", "--ref", "0.3.0", "-s", "README.md", url])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    old_dataset = get_dataset_with_injection("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "update", "--all"], catch_exceptions=False).exit_code

    dataset = get_dataset_with_injection("my-data")
    mutator = get_git_user(project.repository)
    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.parametrize(
    "url",
    [
        "https://dev.renku.ch/datasets/9dde49ee031a4568b193a58892e26534",
        "https://dev.renku.ch/datasets/6baee173933846798a8003f4b1a9a86e",
        "https://dev.renku.ch/datasets/b956c47f91824e6d8d5ac161b97c195c",
        "https://dev.renku.ch/datasets/0dc3a120e4af4a4ca88870d1719c4631",
    ],
)
@retry_failed
def test_import_returns_last_dataset_version(runner, project, url):
    """Test importing with any identifier returns the last version of dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--name", "my-dataset", url]).exit_code

    dataset = get_dataset_with_injection("my-dataset")

    initial_identifier = "9dde49ee031a4568b193a58892e26534"
    latest_identifier = "0dc3a120e4af4a4ca88870d1719c4631"
    assert dataset.identifier not in [initial_identifier, latest_identifier]
    assert f"https://dev.renku.ch/datasets/{latest_identifier}" == dataset.same_as.url["@id"]


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_datasets_provenance_after_import(runner, project):
    """Test dataset provenance is updated after importing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--name", "my-data", "10.7910/DVN/F4NUMR"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        assert datasets_provenance.get_by_slug("my-data") is not None


@pytest.mark.integration
@retry_failed
def test_datasets_provenance_after_git_update(project, runner):
    """Test dataset provenance is updated after an update."""
    url = "https://github.com/SwissDataScienceCenter/renku-jupyter.git"

    result = runner.invoke(cli, ["dataset", "add", "--create", "my-data", "--ref", "0.3.0", "-s", "README.md", url])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    assert 0 == runner.invoke(cli, ["dataset", "update", "--all"], catch_exceptions=False).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        current_version = datasets_provenance.get_by_slug("my-data")
    assert current_version.identifier != current_version.initial_identifier


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_datasets_provenance_after_external_provider_update(project, runner):
    """Test dataset provenance is not updated after an update from an external provider."""
    doi = "10.5281/zenodo.2658634"
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--name", "my-data", doi]).exit_code

    assert 0 == runner.invoke(cli, ["dataset", "update", "my-data"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        current_version = datasets_provenance.get_by_slug("my-data")

    assert current_version.identifier != current_version.initial_identifier


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_datasets_import_with_tag(project, runner):
    """Test dataset import from a Renku provider with a specified tag version."""
    doi = "https://dev.renku.ch/datasets/ddafee6bb38a46f99346cb563afc2c64"
    result = runner.invoke(cli, ["dataset", "import", "-y", "--tag", "v1", doi])

    assert 0 == result.exit_code, format_result_exception(result)

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_slug("parts")

    dataset_path = project.path / "data" / "parts"
    assert "v1" == dataset.version
    assert (dataset_path / "README.md").exists()  # This file was deleted in a later version
    assert doi == dataset.same_as.value
    assert "Updated on 01.06.2022" not in (dataset_path / "parts.csv").read_text()

    git_attributes = (project.repository.path / ".gitattributes").read_text()
    assert "data/parts/parts.csv" in git_attributes
    assert "data/parts/part_relationships.csv" in git_attributes

    result = runner.invoke(cli, ["dataset", "ls-tags", "parts"])

    assert "v1" in result.output
    assert "First version updated on 27.02.2022" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_datasets_imported_with_tag_are_not_updated(project, runner):
    """Test dataset that are imported with a specified tag version won't be updated."""
    doi = "https://dev.renku.ch/datasets/ddafee6bb38a46f99346cb563afc2c64"
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--tag", "v1", doi]).exit_code

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "update", "--all"])

    commit_sha_after = project.repository.head.commit.hexsha

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Skipped updating imported Renku dataset 'parts' with tag 'v1'" in result.output
    assert commit_sha_after == commit_sha_before


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_dataset_update_removes_deleted_files(project, runner, with_injection):
    """Test dataset update removes deleted files in the updated renku datasets."""
    doi = "https://dev.renku.ch/datasets/ddafee6bb38a46f99346cb563afc2c64"
    assert 0 == runner.invoke(cli, ["dataset", "import", "-y", "--tag", "v1", "--name", "parts", doi]).exit_code

    # NOTE: Allow dataset to be updatable by removing ``version`` and setting ``same_as`` to another id of the dataset
    with with_injection():
        with with_dataset(name="parts", commit_database=True) as dataset:
            dataset.version = None
            dataset.same_as = Url(url_id="https://dev.renku.ch/datasets/abc934939cbf45dca0cfef61d05fa132")
    project.repository.add(all=True)
    project.repository.commit("metadata updated")

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_slug("parts")

    assert 4 == len(dataset.files)

    result = runner.invoke(cli, ["dataset", "update", "parts"])

    assert 0 == result.exit_code, format_result_exception(result)
    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_slug("parts")

    assert 2 == len(dataset.files)
    assert {"data/parts/part_categories.csv", "data/parts/parts.csv"} == {f.entity.path for f in dataset.files}


@pytest.mark.integration
def test_dataset_ls_with_tag(runner, tmp_path):
    """Test listing dataset files from a given tag."""
    url = "https://gitlab.dev.renku.ch/renku-python-integration-tests/lego-datasets.git"
    repository = Repository.clone_from(url=url, path=tmp_path / "repo")

    os.chdir(repository.path)

    result = runner.invoke(cli, ["migrate"])

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "ls-files", "--tag", "v1"])

    assert 0 == result.exit_code, format_result_exception(result)
    lines = result.output.split(os.linesep)

    deleted_file = next(line for line in lines if "data/parts/README.md" in line)
    assert "36  B" in deleted_file
    assert "*" not in deleted_file

    deleted_lfs_file = next(line for line in lines if "data/parts/part_relationships.csv" in line)
    assert "548 KB" in deleted_lfs_file
    assert "*" in deleted_lfs_file


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "storage",
    [
        "s3://s3.amazonaws.com/giab/",
        "s3://os.zhdk.cloud.switch.ch/renku-python-test-public/",
        "azure://renkupythontest1.blob.core.windows.net/test-private-1/path",
        "azure://renkupythontest1/test-private-1/path",
        pytest.lazy_fixture("external_cloud_storage"),
    ],
)
def test_create_with_could_storage(runner, project, cloud_storage_credentials, external_cloud_storage, storage):
    """Test creating a dataset with a valid backend cloud storage."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("cloud-data")

    assert storage == dataset.storage

    # NOTE: Dataset's data dir is git-ignored
    dataset_datadir = os.path.join(DATA_DIR, "cloud-data")
    assert {dataset_datadir} == set(project.repository.get_ignored_paths(dataset_datadir))


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_create_with_non_existing_s3_backend(runner, project):
    """Test creating a dataset with an invalid S3 backend storage."""
    result = runner.invoke(
        cli, ["dataset", "create", "s3-data", "--storage", "s3://s3.amazonaws.com/no-giab/"], input="\n\n\n"
    )

    assert 2 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "S3 bucket 'no-giab' doesn't exists" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
def test_create_with_non_existing_azure_backend(runner, project, cloud_storage_credentials):
    """Test creating a dataset with an invalid Azure cloud storage."""
    result = runner.invoke(
        cli, ["dataset", "create", "cloud-data", "--storage", "azure://renkupythontest1/non-existing/"], input="\n\n\n"
    )

    assert 2 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Azure container 'non-existing' doesn't exists" in result.output


@pytest.mark.integration
@pytest.mark.shaky
def test_create_with_non_existing_local_backend(runner, project):
    """Test creating a dataset with a non-existing local backend storage."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", "/non-existing/path"])

    assert 2 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "External path '/non-existing/path' doesn't exists." in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "storage",
    [
        "s3://s3.amazonaws.com/amazon/",
        "azure://renkupythontest1/test-private-1",
    ],
)
def test_create_with_unauthorized_cloud_storage(runner, project, storage):
    """Test creating a dataset with an invalid credentials."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 1 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
    assert "Authentication failed when accessing the cloud storage" in result.output


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "storage, files",
    [
        [
            "s3://s3.amazonaws.com/giab/",
            [
                ("Aspera_download_from_ftp.README", "e8530c02585aaaecba2d5bd6c4cea6ae"),
                ("technical/unimask/02structural.bed.gz", "0ddc10ab9f9f0dd0fea4d66d9a55ba99"),
            ],
        ],
        [
            "azure://renkupythontest1/test-private-1",
            [
                ("file-1", "ba240f743099afb725adcc0e267b2987"),
                ("directory-1/file-2", "e984bdba4a20181ef40f1bdc9ca82865"),
            ],
        ],
        [
            pytest.lazy_fixture("external_cloud_storage"),
            [("file1", "9d98eede4ccb193e379d6dbd7cc1eb86"), ("dir1/file2", "7bec9352114f8139c2640b2554563508")],
        ],
    ],
)
def test_pull_data_from_cloud_storage(
    runner, project, cloud_storage_credentials, external_cloud_storage, storage: str, files
):
    """Test pulling data for a dataset with cloud storage backend."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    file_1, hash_1 = files[0]
    file_1 = os.path.join(storage, file_1)
    file_2, hash_2 = files[1]
    file_2 = os.path.join(storage, file_2)

    result = runner.invoke(cli, ["dataset", "add", "cloud-data", "--copy", file_1, file_2])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    result = runner.invoke(cli, ["dataset", "pull", "cloud-data"])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("cloud-data")
    file = next(f for f in dataset.files if f.entity.path.endswith(Path(file_1).name))
    assert hash_1 == file.based_on.checksum

    assert (project.path / file.entity.path).exists()
    assert not (project.path / file.entity.path).is_symlink()

    file = next(f for f in dataset.files if f.entity.path.endswith(Path(file_2).name))

    assert (project.path / file.entity.path).exists()
    assert not (project.path / file.entity.path).is_symlink()
    assert hash_2 == file.based_on.checksum


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.vcr
@pytest.mark.parametrize(
    "storage, files",
    [
        [
            "s3://s3.amazonaws.com/giab/",
            [
                ("Aspera_download_from_ftp.README", "e8530c02585aaaecba2d5bd6c4cea6ae"),
                ("technical/unimask/02structural.bed.gz", "0ddc10ab9f9f0dd0fea4d66d9a55ba99"),
            ],
        ],
        [
            "azure://renkupythontest1/test-private-1",
            [
                ("file-1", "ba240f743099afb725adcc0e267b2987"),
                ("directory-1/file-2", "e984bdba4a20181ef40f1bdc9ca82865"),
            ],
        ],
        [
            pytest.lazy_fixture("external_cloud_storage"),
            [("file1", "9d98eede4ccb193e379d6dbd7cc1eb86"), ("dir1/file2", "7bec9352114f8139c2640b2554563508")],
        ],
    ],
)
def test_pull_data_from_cloud_storage_to_a_location(
    runner, project, cloud_storage_credentials, tmp_path, external_cloud_storage, storage: str, files
):
    """Test pulling data for a dataset with cloud storage to a location other than dataset's data directory."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    file_1, hash_1 = files[0]
    file_1 = os.path.join(storage, file_1)
    file_2, hash_2 = files[1]
    file_2 = os.path.join(storage, file_2)

    result = runner.invoke(cli, ["dataset", "add", "cloud-data", "--copy", file_1, file_2])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    location = tmp_path / "cloud-data"

    result = runner.invoke(cli, ["dataset", "pull", "cloud-data", "--location", str(location)], input="y")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    dataset = get_dataset_with_injection("cloud-data")
    assert (project.path / dataset.get_datadir()).is_symlink()

    filename = Path(file_1).name
    file = next(f for f in dataset.files if f.entity.path.endswith(filename))

    assert (location / filename).resolve() == (project.path / file.entity.path).resolve()
    assert hash_1 == file.based_on.checksum

    filename = Path(file_2).name
    file = next(f for f in dataset.files if f.entity.path.endswith(filename))

    assert (location / filename).resolve() == (project.path / file.entity.path).resolve()
    assert hash_2 == file.based_on.checksum


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.parametrize(
    "args, files, storage",
    [
        (["--create", "--storage", "s3://s3.amazonaws.com/giab"], ["s3://s3.amazonaws.com/giab"], None),
        ([], ["tools", "use_cases"], "s3://s3.amazonaws.com/giab"),
        ([], [""], "s3://s3.amazonaws.com/giab"),
        ([], ["tools", "changelog_details"], "s3://s3.amazonaws.com/giab"),
        (
            ["--create", "--storage", "azure://renkupythontest1/test-private-1"],
            ["azure://renkupythontest1/test-private-1"],
            None,
        ),
        ([], ["file-1", "use_cases"], "azure://renkupythontest1/test-private-1"),
        ([], [""], "azure://renkupythontest1/test-private-1"),
        ([], ["file-1", "changelog_details"], "azure://renkupythontest1/test-private-1"),
        (["--copy"], ["file1", "dir1/file2"], pytest.lazy_fixture("external_cloud_storage")),
    ],
)
def test_adding_data_from_cloud_storage(
    runner, project, create_cloud_storage_dataset, external_cloud_storage, mocker, args, files, storage
):
    """Ensure metadata from a bucket can be added."""
    if storage:
        files = [os.path.join(storage, f).rstrip("/") for f in files]

    def get_path(uri):
        return re.sub(r".*://", "", uri).lstrip("/")  # Remove scheme and make sure path isn't absolute

    mock_cloud_storage = mocker.patch("renku.infrastructure.storage.factory.StorageFactory.get_storage", autospec=True)
    instance_cloud_storage = mock_cloud_storage.return_value
    dataset_slug = "cloud-data"
    instance_cloud_storage.get_hashes.return_value = [FileHash(uri=f, path=get_path(f), size=42, hash=f) for f in files]
    if storage:
        result = create_cloud_storage_dataset(dataset_slug, storage)
        assert result.exit_code == 0, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", dataset_slug, *args, *files], input="\n\nn\n")
    assert result.exit_code == 0, format_result_exception(result)

    assert instance_cloud_storage.get_hashes.call_count == len(files)
    result = runner.invoke(cli, ["dataset", "ls-files"])

    assert result.exit_code == 0, format_result_exception(result)
    assert re.sub(r".*://", "", files[0]) in result.stdout, result.stdout
    assert all([re.sub(r".*://", "", f) in result.stdout for f in files])


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.parametrize(
    "storage, cmd_args, expected_error_msg",
    [
        (
            "s3://s3.amazonaws.com/giab",
            ["s3://s3.amazonaws.com/giab", "--storage", "s3://s3.amazonaws.com/giab"],
            "Storage can be set only when creating a dataset",
        ),
        (
            "s3://s3.amazonaws.com/giab",
            ["https://github.com/SwissDataScienceCenter/renku-python/raw/develop/README.rst"],
            "does not match the defined storage url",
        ),
        (
            "s3://s3.amazonaws.com/giab",
            ["s3://s3.amazonaws.com/giab/*"],
            "Wildcards like '*' or '?' are not supported for cloud storage URIs",
        ),
        (
            "s3://s3.amazonaws.com/giab",
            ["s3://s3.amazonaws.com/giab/test?.txt"],
            "Wildcards like '*' or '?' are not supported for cloud storage URIs",
        ),
        (
            "s3://s3.amazonaws.com/giab",
            ["s3://s3.amazonaws.com/giab", "--source", "tools"],
            "Cannot use '-s/--src/--source' with URLs or local files",
        ),
        (
            "azure://renkupythontest1/test-private-1",
            ["azure://renkupythontest1/test-private-1", "--storage", "azure://renkupythontest1/test-private-1"],
            "Storage can be set only when creating a dataset",
        ),
        (
            "azure://renkupythontest1/test-private-1",
            ["https://github.com/SwissDataScienceCenter/renku-python/raw/develop/README.rst"],
            "does not match the defined storage url",
        ),
        (
            "azure://renkupythontest1/test-private-1",
            ["azure://renkupythontest1/test-private-1/*"],
            "Wildcards like '*' or '?' are not supported for cloud storage URIs",
        ),
        (
            "azure://renkupythontest1/test-private-1",
            ["azure://renkupythontest1/test-private-1/test?.txt"],
            "Wildcards like '*' or '?' are not supported for cloud storage URIs",
        ),
        (
            "azure://renkupythontest1/test-private-1",
            ["azure://renkupythontest1/test-private-1", "--source", "tools"],
            "Cannot use '-s/--src/--source' with URLs or local files",
        ),
    ],
)
def test_invalid_cloud_storage_args(
    runner, project, create_cloud_storage_dataset, storage, cmd_args, expected_error_msg, mocker
):
    """Test invalid arguments for adding data to S3 dataset."""
    mock_cloud_storage_storage = mocker.patch(
        "renku.infrastructure.storage.factory.StorageFactory.get_storage", autospec=True
    )
    dataset_slug = "test-cloud-dataset"
    if "--create" not in cmd_args:
        instance_cloud_storage_storage = mock_cloud_storage_storage.return_value
        res = create_cloud_storage_dataset(dataset_slug, storage)
        assert res.exit_code == 0
        instance_cloud_storage_storage.exists.assert_called_with(storage)

    res = runner.invoke(cli, ["dataset", "add", dataset_slug, *cmd_args])

    assert res.exit_code != 0
    assert expected_error_msg in res.stderr


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.serial
@retry_failed
@pytest.mark.parametrize(
    "storage, path",
    [
        ("s3://s3.amazonaws.com/giab/", "Aspera_download_from_ftp.README"),
        ("azure://renkupythontest1/test-private-1", "file-1"),
        (pytest.lazy_fixture("external_cloud_storage"), "file1"),
    ],
)
def test_mount_unmount_data_from_cloud_storage(
    runner, project, cloud_storage_credentials, external_cloud_storage, storage, path
):
    """Test mounting/unmounting data for a dataset with cloud storage backend."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    cloud_data = project.path / "data" / "cloud-data" / path
    assert not cloud_data.exists()

    # NOTE: Create some dummy files
    dummy = project.path / "data" / "cloud-data" / "dummy"
    dummy.parent.mkdir(exist_ok=True, parents=True)
    dummy.touch()

    try:
        result = runner.invoke(cli, ["dataset", "mount", "cloud-data"], input="y")
        time.sleep(1)

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert "Warning: Dataset's data directory will be removed: data/cloud-data." in result.output
        assert cloud_data.exists()
    finally:
        result = runner.invoke(cli, ["dataset", "mount", "cloud-data", "--unmount"])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert not cloud_data.exists()


@pytest.mark.integration
@pytest.mark.shaky
@pytest.mark.serial
@retry_failed
@pytest.mark.parametrize(
    "storage, path, rclone_uri, env",
    [
        (
            "s3://s3.amazonaws.com/giab/",
            "Aspera_download_from_ftp.README",
            "s3://giab/",
            {"RCLONE_CONFIG_S3_TYPE": "s3", "RCLONE_CONFIG_S3_PROVIDER": "AWS"},
        ),
        (
            "azure://renkupythontest1/test-private-1",
            "file-1",
            "azure://test-private-1/",
            {
                "RCLONE_CONFIG_AZURE_TYPE": "azureblob",
                "RCLONE_CONFIG_AZURE_ACCOUNT": "renkupythontest1",
                "RCLONE_CONFIG_AZURE_KEY": os.getenv("CLOUD_STORAGE_AZURE_KEY", ""),
            },
        ),
        (
            pytest.lazy_fixture("external_cloud_storage"),
            "file1",
            None,
            {"RCLONE_CONFIG_FILE_TYPE": "local"},
        ),
    ],
)
def test_mount_data_from_an_existing_mount_point(
    runner, project, tmp_path, cloud_storage_credentials, external_cloud_storage, storage, path, rclone_uri, env
):
    """Test get data for a dataset with cloud storage backend from an existing mount-point."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    cloud_data = project.path / "data" / "cloud-data" / path
    assert not cloud_data.exists()

    # NOTE: Create some dummy files
    dummy = project.path / "data" / "cloud-data" / "dummy"
    dummy.parent.mkdir(exist_ok=True, parents=True)
    dummy.touch()

    mount_point = tmp_path / "cloud-mount"
    mount_point.mkdir(exist_ok=True, parents=True)
    os_env = os.environ.copy()
    os_env.update(env)
    rclone_uri = rclone_uri or f"file://{storage}"
    subprocess.run(
        ["rclone", "mount", "--read-only", "--no-modtime", "--daemon", rclone_uri, str(mount_point)], env=os_env
    )

    try:
        result = runner.invoke(cli, ["dataset", "mount", "cloud-data", "--yes", "--existing", str(mount_point)])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert "Warning: Dataset's data directory will be removed: data/cloud-data." not in result.output
        assert cloud_data.exists()

        datadir = cloud_data.parent
        assert datadir.is_symlink()
        assert datadir.resolve() == mount_point.resolve()

        result = runner.invoke(cli, ["dataset", "unmount", "cloud-data"])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert not cloud_data.exists()
        assert not datadir.exists()

        cloud_data = tmp_path / "cloud-mount" / path
        assert cloud_data.exists()
    finally:
        unmount_path(mount_point)


@pytest.mark.integration
@pytest.mark.shaky
@retry_failed
@pytest.mark.parametrize(
    "storage",
    [
        "s3://os.zhdk.cloud.switch.ch/renku-python-integration-test",
        "azure://renkupythontest1/test-private-1/renku-python-test",
        pytest.lazy_fixture("external_cloud_storage"),
    ],
)
def test_add_data_to_mounted_cloud_storage(
    runner, project, tmp_path, cloud_storage_credentials, external_cloud_storage, storage
):
    """Test add data to datasets with read-only mounted cloud storage."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    cloud_data = project.path / "data" / "cloud-data"

    local_file = tmp_path / "local-file.txt"
    random_data = str(random.random())
    local_file.write_text(random_data)

    try:
        assert not cloud_data.exists()

        result = runner.invoke(cli, ["dataset", "mount", "cloud-data", "--yes"])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert cloud_data.exists()

        result = runner.invoke(cli, ["dataset", "add", "--copy", "cloud-data", local_file])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

        # NOTE: It takes a while to sync; we unmount/mount to make changes visible
        runner.invoke(cli, ["dataset", "mount", "cloud-data", "--unmount"])
        runner.invoke(cli, ["dataset", "pull", "cloud-data"])

        copied_data = cloud_data / "local-file.txt"
        assert copied_data.exists()
        assert random_data == copied_data.read_text()
    finally:
        result = runner.invoke(cli, ["dataset", "mount", "cloud-data", "--unmount"])

        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)


@pytest.mark.integration
@pytest.mark.shaky
def test_add_data_from_local_cloud_storage(runner, project, external_cloud_storage):
    """Test adding data from local cloud storage copies data to correct destination."""
    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", external_cloud_storage])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    file_1 = f"{external_cloud_storage}/file1"
    file_2 = f"{external_cloud_storage}/dir1/file2"

    result = runner.invoke(cli, ["dataset", "add", "cloud-data", "--copy", file_1, file_2])

    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    file_1_destination = file_1
    file_2_destination = f"{external_cloud_storage}/file2"

    assert Path(file_1_destination).exists()
    assert Path(file_2_destination).exists()
