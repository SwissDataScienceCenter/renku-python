# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
import os
from pathlib import Path

import git
import pytest

from renku.cli import cli
from renku.cli.init import create_template_sentence, parse_parameters
from renku.core import errors
from tests.utils import raises


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


def test_template_selection_helpers():
    templates = [
        {"name": "Template Python", "folder": "folder_python", "description": "Description Python"},
        {
            "name": "Template R",
            "folder": "folder_R",
            "description": "Description R",
            "variables": {"custom": "random data"},
        },
    ]
    instructions = "Please choose a template by typing the index"
    sentence = create_template_sentence(templates)
    stripped_sentence = " ".join(sentence.split())

    assert "1 folder_python" in stripped_sentence
    assert "2 folder_R" in stripped_sentence
    assert instructions not in stripped_sentence
    full_sentence = create_template_sentence(templates, instructions=True)
    assert instructions in full_sentence

    # with describe=True
    sentence = create_template_sentence(templates, describe=True)
    stripped_sentence = " ".join(sentence.split())

    assert "1 folder_python Description Python" in stripped_sentence
    assert "2 folder_R custom: random data Description R" in stripped_sentence
    assert instructions not in stripped_sentence

    full_sentence = create_template_sentence(templates, describe=True, instructions=True)
    assert instructions in full_sentence


def test_list_templates(isolated_runner, project_init, template):
    """Test listing templates."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["list"])
    assert 0 == result.exit_code
    assert not new_project.exists()
    assert template["id"] in result.output


def test_init(isolated_runner, project_init):
    """Test project initialization from template."""
    data, commands = project_init

    # create the project
    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata.yml").exists()

    # try to re-create in the same folder
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 != result.exit_code

    # force re-create in the same folder
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata.yml").exists()

    # init using index instead of id
    new_project_2 = Path(data["test_project_alt"])
    result = isolated_runner.invoke(cli, commands["init_alt"] + commands["index"], commands["confirm"])
    assert 0 == result.exit_code
    assert new_project_2.exists()
    assert (new_project_2 / ".renku").exists()
    assert (new_project_2 / ".renku" / "renku.ini").exists()
    assert (new_project_2 / ".renku" / "metadata.yml").exists()

    # verify both init lead to the same result
    template_files = [f for f in new_project.glob("**/*") if ".git" not in str(f)]
    for template_file in template_files:
        expected_file = new_project_2 / template_file.relative_to(new_project)
        assert expected_file.exists()

    # verify providing both index and id fails
    result = isolated_runner.invoke(
        cli, commands["init_alt"] + commands["index"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 2 == result.exit_code
    assert "Use either --template-id or --template-index, not both" in result.output


@pytest.mark.parametrize(
    "remote",
    [
        (
            "https://user:password@dev.renku.ch/gitlab/group/subgroup/project.git",
            "https://dev.renku.ch/projects/group/subgroup/project",
        ),
        ("ssh://@dev.renku.ch:group/subgroup/project.git", "https://dev.renku.ch/projects/group/subgroup/project"),
        (
            "https://user:password@dev.renku.ch/gitlab/group/subgroup/subsubgroup/project.git",
            "https://dev.renku.ch/projects/group/subgroup/subsubgroup/project",
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
    repo = git.Repo.init(new_project)
    repo.create_remote("origin", remote[0])
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata.yml").exists()
    assert remote[1] in (new_project / ".renku" / "metadata.yml").read_text()


def test_init_force_in_empty_dir(isolated_runner, project_init):
    """Run init --force in empty directory."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code


def test_init_force_in_dirty_dir(isolated_runner, project_init):
    """Run init --force in dirty directory."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()

    new_project.mkdir(parents=True)
    random_file = new_project / "random_file.txt"
    with random_file.open("w") as dest:
        dest.writelines(["random text"])
    assert random_file.exists()

    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    lines = result.output.split("\n")
    assert 1 == result.exit_code
    assert "is not empty" in lines[0]
    assert "random_file.txt" in lines[1]

    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert random_file.exists()
    assert 0 == result.exit_code


def test_init_on_cloned_repo(isolated_runner, data_repository, project_init):
    """Run init --force in directory containing another repo."""
    data, commands = project_init

    new_project = Path(data["test_project"])
    import shutil

    shutil.copytree(str(data_repository.working_dir), str(new_project))
    assert new_project.exists()

    # try to create in a dirty folder
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 != result.exit_code

    # force re-create in the same folder
    result = isolated_runner.invoke(
        cli, commands["init_test"] + commands["id"] + commands["force"], commands["confirm"]
    )
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata.yml").exists()


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
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / ".renku").exists()
    assert (new_project / ".renku" / "renku.ini").exists()
    assert (new_project / ".renku" / "metadata.yml").exists()


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
    assert 0 == result.exit_code
    assert "The template requires a value for" in result.output
    for param in set(template["metadata"].keys()):
        assert param in result.output
    assert "These parameters are not used by the template and were ignored:" in result.output


@pytest.mark.parametrize("data_dir", ["dir", "nested/dir/s"])
def test_init_with_data_dir(isolated_runner, data_dir, directory_tree, project_init):
    """Test initializing with data directory."""
    from git import Repo

    data, commands = project_init

    new_project = Path(data["test_project"])
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"] + ["--data-dir", data_dir])
    assert 0 == result.exit_code

    assert (new_project / data_dir).exists()
    assert (new_project / data_dir / ".gitkeep").exists()
    assert not Repo(new_project).is_dirty()

    os.chdir(new_project.resolve())
    result = isolated_runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, result.output
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


def test_default_init_parameters(isolated_runner, mocker, project_init, template):
    """Test that the default parameters are set in template initialisation."""
    create_from_template = mocker.patch("renku.cli.init.create_from_template")
    mocker.patch("renku.cli.githooks.install")

    data, commands = project_init

    new_project = Path(data["test_project"])
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, commands["init_test"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code
    create_from_template.assert_called_once()
    metadata = create_from_template.call_args[1]["metadata"]
    assert {
        "__template_source__",
        "__template_ref__",
        "__template_id__",
        "__namespace__",
        "__repository__",
        "__project_slug__",
        "__sanitized_project_name__",
    } <= set(metadata.keys())
    assert metadata["__template_source__"] == "renku"
    assert metadata["__template_ref__"] == "master"
    assert metadata["__template_id__"] == template["id"]
    assert metadata["__namespace__"] == ""
    assert metadata["__repository__"] == ""
    assert metadata["__project_slug__"] == ""
    assert metadata["__sanitized_project_name__"] == ""
