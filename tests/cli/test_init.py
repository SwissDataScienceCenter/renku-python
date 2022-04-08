# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Test ``init`` command."""

import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

import pytest

from renku.core import errors
from renku.infrastructure.database import Database
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from renku.ui.cli.init import parse_parameters
from tests.utils import format_result_exception, raises


def test_parse_parameters(project_init):
    def clean_param(p):
        return [v for v in p if v != "--parameter"]

    data, commands = project_init

    parsed = parse_parameters(None, None, clean_param(commands["parameters"]))
    keys = parsed.keys()
    assert 2 == len(keys)
    assert "p1" in keys
    assert "p2" in keys
    assert "v1" == parsed["p1"]
    assert "v2" == parsed["p2"]
    with raises(errors.ParameterError):
        parse_parameters(
            None, None, clean_param(commands["parameters"]) + clean_param(commands["parameters_equal_missing"])
        )
    with raises(errors.ParameterError):
        parse_parameters(
            None, None, clean_param(commands["parameters"]) + clean_param(commands["parameters_equal_early"])
        )


def test_template_selection_helpers(isolated_runner):
    """Test template selection is displayed."""
    url = "https://github.com/SwissDataScienceCenter/renku-project-template"
    result = isolated_runner.invoke(cli, ["init", "-s", url, "-r", "0.3.0"], "2\n")

    stripped_output = " ".join(result.output.split())

    assert "Please choose a template by typing its index:" in stripped_output

    assert "1 python-minimal" in stripped_output
    assert "2 R-minimal" in stripped_output


def test_init(isolated_runner, project_init):
    """Test project initialization from template."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()

    # try to re-create in the same folder
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 != result.exit_code

    # force re-create in the same folder
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()

    # init using index instead of id
    new_project_2 = Path(data["test_project_alt"])
    result = isolated_runner.invoke(cli, commands["init_alt"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project_2.exists()
    assert (new_project_2 / ".renku").exists()
    assert (new_project_2 / ".renku" / "renku.ini").exists()
    assert (new_project_2 / ".renku" / "metadata").exists()

    # verify both init lead to the same result
    template_files = [f for f in new_project.glob("**/*") if ".git" not in str(f) and ".renku/metadata/" not in str(f)]
    for template_file in template_files:
        expected_file = new_project_2 / template_file.relative_to(new_project)
        assert expected_file.exists()


def test_init_with_template_index(isolated_runner, project_init):
    """Test initialization with --template-index is deprecated."""
    _, commands = project_init

    # verify providing both index and id fails
    result = isolated_runner.invoke(cli, commands["init_alt"] + commands["index"] + commands["force"])

    assert 2 == result.exit_code
    assert "'-i/--template-index' is deprecated: Use '-t/--template-id' to pass a template id" in result.output


def test_init_initial_branch(isolated_runner, project_init):
    """Test project initialization from template."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["initial_branch_main"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()
    assert Repository(new_project).active_branch.name == data["main_branch"]


@pytest.mark.parametrize(
    "remote",
    [
        (
            "https://user:password@dev.renku.ch/gitlab/group/subgroup/project.git",
            "https://dev.renku.ch/projects/group/subgroup/project",
        ),
        ("ssh://@dev.renku.ch:group/subgroup/project.git", "https://dev.renku.ch/projects/group/subgroup/project"),
        (
            "https://user:password@dev.renku.ch/gitlab/group/subgroup/sub-subgroup/project.git",
            "https://dev.renku.ch/projects/group/subgroup/sub-subgroup/project",
        ),
        (
            "https://user:password@dev.renku.ch/group/subgroup/project.git",
            "https://dev.renku.ch/projects/group/subgroup/project",
        ),
        ("https://user:password@dev.renku.ch/user/project.git", "https://dev.renku.ch/projects/user/project"),
    ],
)
def test_init_with_git_remote(isolated_runner, project_init, remote):
    """Test project initialization with remote and possibly gitlab groups set."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    new_project.mkdir()
    repository = Repository.initialize(new_project)
    repository.remotes.add("origin", remote[0])
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()

    url = urlparse(remote[1])
    assert url.path in (new_project / ".renku" / "metadata" / "project").read_text()


def test_init_force_in_empty_dir(isolated_runner, project_init):
    """Run init --force in empty directory."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)


def test_init_force_in_dirty_dir(isolated_runner, project_init):
    """Run init --force in dirty directory."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()

    new_project.mkdir(parents=True)
    random_file = new_project / "random_file.txt"
    with random_file.open("w") as destination:
        destination.writelines(["random text"])
    assert random_file.exists()

    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Project initialized" in result.output

    shutil.rmtree(new_project)

    new_project.mkdir(parents=True)

    dockerfile = new_project / "Dockerfile"
    with dockerfile.open("w") as destination:
        destination.writelines(["not a dockerfile"])
    assert dockerfile.exists()

    readme = new_project / "README.md"
    with readme.open("w") as destination:
        destination.writelines(["My first project!"])
    assert readme.exists()

    random_file = new_project / "random_file.txt"
    with random_file.open("w") as destination:
        destination.writelines(["random text"])
    assert random_file.exists()

    gitignore = new_project / ".gitignore"
    with gitignore.open("w") as destination:
        destination.writelines(["dummy text that's definitely not in the actual gitignore"])
    assert gitignore.exists()

    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])

    assert 1 == result.exit_code
    assert "The following files exist in the directory and will be overwritten" in result.output
    assert "The following files exist in the directory and will be appended to" in result.output
    assert "\tDockerfile\n" in result.output
    assert "\t.gitignore\n" in result.output

    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)

    assert random_file.exists()
    assert dockerfile.exists()
    assert "not a dockerfile" not in dockerfile.read_text()
    assert gitignore.exists()
    assert "dummy text that's definitely not in the actual gitignore" in gitignore.read_text()
    assert readme.exists()
    assert "My first project!" == readme.read_text()


def test_init_on_cloned_repo(isolated_runner, data_repository, project_init):
    """Run init --force in directory containing another repository."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    import shutil

    shutil.copytree(str(data_repository.path), str(new_project))
    assert new_project.exists()

    # try to create in a dirty folder
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()


@pytest.mark.integration
def test_init_remote(isolated_runner, project_init):
    """Test project initialization from a remote template."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata").exists()


@pytest.mark.integration
def test_init_new_metadata_defaults(isolated_runner, project_init):
    """Test project initialization from a remote template with defaults doesn't prompt for those values."""
    data, commands = project_init
    template_source = ["--template-source", commands["init_custom_template"]]

    # NOTE: Set values for parameters that don't have default values
    parameters = ["-p", "description=some description", "-p", "number_val=70.12"]

    result = isolated_runner.invoke(cli, commands["init_custom"] + template_source + parameters)

    assert 0 == result.exit_code, format_result_exception(result)

    project = Database.from_path(Path(data["test_project"]) / ".renku" / "metadata").get("project")
    metadata = json.loads(project.template_metadata)
    assert True is metadata["bool_var"]
    assert "ask again" == metadata["enum_var"]
    assert "some description" == metadata["description"]
    assert 70.12 == metadata["number_val"]

    assert "Enter a value for" not in result.output


@pytest.mark.integration
def test_init_new_metadata_defaults_is_overwritten(isolated_runner, project_init):
    """Test passed parameters overwrite default parameters values."""
    data, commands = project_init
    template_source = ["--template-source", commands["init_custom_template"]]

    parameters = ["-p", "description=some description", "-p", "number_val=70.12"]
    parameters += ["-p", "enum_var=maybe", "-p", "bool_var=false"]

    result = isolated_runner.invoke(cli, commands["init_custom"] + template_source + parameters)

    assert 0 == result.exit_code, format_result_exception(result)

    project = Database.from_path(Path(data["test_project"]) / ".renku" / "metadata").get("project")
    metadata = json.loads(project.template_metadata)
    assert False is metadata["bool_var"]
    assert "maybe" == metadata["enum_var"]
    assert "some description" == metadata["description"]
    assert 70.12 == metadata["number_val"]

    assert "Enter a value for" not in result.output


@pytest.mark.integration
def test_init_new_metadata_invalid_param_value(isolated_runner, project_init):
    """Test project initialization from a remote template."""
    data, commands = project_init
    template_source = ["--template-source", commands["init_custom_template"]]

    # NOTE: Set values for parameters
    parameters = ["-p", "description=some description", "-p", "number_val=invalid-number"]

    result = isolated_runner.invoke(cli, commands["init_custom"] + template_source + parameters, "42\n")

    assert 0 == result.exit_code, format_result_exception(result)

    assert "Enter a value for 'number_val'" in result.output
    assert "Enter a value for 'bool_var'" not in result.output


def test_init_with_parameters(isolated_runner, project_init, template):
    """Test project initialization using custom metadata."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["parameters"] + commands["parameters_equal_missing"]
    )
    assert 0 != result.exit_code
    assert (
        f'Error: Invalid parameter value for --parameter "{ commands["parameters_equal_missing"][1]}"' in result.output
    )

    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["parameters"] + commands["parameters_equal_early"]
    )
    assert 0 != result.exit_code
    assert f'Error: Invalid parameter value for --parameter "{commands["parameters_equal_early"][1]}"' in result.output

    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["parameters"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    # TODO: Re-enable this check once parameters are added to the template.
    # assert "The template requires a value for" in result.output
    for param in set(template["metadata"].keys()):
        assert param in result.output
    assert "These parameters are not used by the template and were ignored:" in result.output


def test_init_with_custom_metadata(isolated_runner, project_init, template):
    """Test project initialization using custom metadata."""
    data, commands = project_init

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = Path("metadata.json")
    metadata_path.write_text(json.dumps(metadata))

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"] + ["--metadata", str(metadata_path)])
    assert 0 == result.exit_code

    database = Database.from_path(new_project / ".renku" / "metadata")
    project = database.get("project")

    assert metadata == project.annotations[0].body


@pytest.mark.parametrize("data_dir", ["dir", "nested/dir/s"])
def test_init_with_data_dir(isolated_runner, data_dir, directory_tree, project_init):
    """Test initializing with data directory."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"] + ["--data-dir", data_dir])
    assert 0 == result.exit_code, format_result_exception(result)

    assert (new_project / data_dir).exists()
    assert (new_project / data_dir / ".gitkeep").exists()
    assert not Repository(new_project).is_dirty(untracked_files=True)

    os.chdir(new_project.resolve())
    result = isolated_runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert (Path(data_dir) / "my-data" / directory_tree.name / "file1").exists()


@pytest.mark.parametrize("data_dir", ["/absolute/path/outside", "../relative/path/outside"])
def test_init_with_wrong_data_dir(isolated_runner, data_dir, project_init):
    """Test initialization fails with wrong data directory."""
    data, commands = project_init

    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"] + ["--data-dir", data_dir])
    assert 2 == result.exit_code
    assert f"Data directory {data_dir} is not within project" in result.output


@pytest.mark.parametrize("data_dir", [".", ".git", ".renku", ".git/"])
def test_init_with_invalid_data_dir(isolated_runner, data_dir, project_init):
    """Test initialization fails with invalid data directory."""
    data, commands = project_init

    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"] + ["--data-dir", data_dir])
    assert 2 == result.exit_code
    data_dir = data_dir.rstrip("/")
    assert f"Cannot use {data_dir} as data directory." in result.output


def test_init_with_description(isolated_runner, template):
    """Test project initialization with description."""
    result = isolated_runner.invoke(
        cli, ["init", "--description", "my project description", "new project", "--template-id", template["id"]]
    )

    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(Path("new project") / ".renku" / "metadata")
    project = database.get("project")

    assert "new project" == project.name
    assert project.id.endswith("new-project")  # make sure id uses slug version of name without space
    assert "my project description" in project.template_metadata
    assert "my project description" == project.description

    readme_content = (Path("new project") / "README.md").read_text()
    assert "my project description" in readme_content

    os.chdir("new project")
    result = isolated_runner.invoke(cli, ["graph", "export", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my project description" in result.output
