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
"""Test ``status`` command."""

import os
from pathlib import Path

from renku.command.status import get_status_command
from renku.core.project.project_properties import project_properties
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


def test_status(runner, project, subdirectory):
    """Test status check."""
    source = os.path.join(project, "source.txt")
    output = os.path.join(project, "data", "output.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "cp", source, output])
    assert 0 == result.exit_code, format_result_exception(result)

    result = get_status_command().build().execute().output
    assert not (
        result.outdated_outputs or result.outdated_activities or result.modified_inputs or result.deleted_inputs
    )

    write_and_commit_file(repo, source, "new content")

    result = get_status_command().build().execute().output

    assert (os.path.relpath(output), {os.path.relpath(source)}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {os.path.relpath(source)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_multiple_steps(runner, project):
    """Test status check with multiple steps."""
    source = os.path.join(os.getcwd(), "source.txt")
    intermediate = os.path.join(os.getcwd(), "intermediate.txt")
    output = os.path.join(os.getcwd(), "data", "output.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source, "content")

    assert 0 == runner.invoke(cli, ["run", "cp", source, intermediate]).exit_code
    assert 0 == runner.invoke(cli, ["run", "cp", intermediate, output]).exit_code

    write_and_commit_file(repo, source, "new content")

    result = get_status_command().build().execute().output

    assert ("data/output.txt", {"source.txt"}) in result.outdated_outputs.items()
    assert ("intermediate.txt", {"source.txt"}) in result.outdated_outputs.items()
    assert 2 == len(result.outdated_outputs)
    assert {"source.txt"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_workflow_without_outputs(runner, project):
    """Test workflow without outputs."""
    source = os.path.join(os.getcwd(), "source.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "cat", "--no-output", source])
    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(repo, source, "new content")

    result = get_status_command().build().execute().output

    assert 0 == len(result.outdated_outputs)
    assert {"source.txt"} == result.modified_inputs
    assert 1 == len(result.outdated_activities)
    assert "/activities/" in list(result.outdated_activities.keys())[0]
    assert 0 == len(result.deleted_inputs)


def test_status_with_paths(runner, project, subdirectory):
    """Test status check with multiple steps."""
    source1 = os.path.join(project, "source1.txt")
    output1 = os.path.join(project, "data", "output1.txt")
    source2 = os.path.join(project, "source2.txt")
    output2 = os.path.join(project, "data", "output2.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source1, "content")
    write_and_commit_file(repo, source2, "content")

    assert 0 == runner.invoke(cli, ["run", "cp", source1, output1]).exit_code
    assert 0 == runner.invoke(cli, ["run", "cp", source2, output2]).exit_code

    write_and_commit_file(repo, source1, "new content")
    write_and_commit_file(repo, source2, "new content")

    result = get_status_command().build().execute(paths=[source1]).output

    assert (os.path.relpath(output1), {os.path.relpath(source1)}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {os.path.relpath(source1)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)

    result = get_status_command().build().execute(paths=[output1]).output

    assert (os.path.relpath(output1), {os.path.relpath(source1)}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {os.path.relpath(source1)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)

    result = get_status_command().build().execute(paths=[source2]).output

    assert (os.path.relpath(output2), {os.path.relpath(source2)}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {os.path.relpath(source2)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)

    result = get_status_command().build().execute(paths=[output2]).output

    assert (os.path.relpath(output2), {os.path.relpath(source2)}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {os.path.relpath(source2)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)

    result = get_status_command().build().execute(paths=[source1, output2]).output

    assert (os.path.relpath(output1), {os.path.relpath(source1)}) in result.outdated_outputs.items()
    assert (os.path.relpath(output2), {os.path.relpath(source2)}) in result.outdated_outputs.items()
    assert 2 == len(result.outdated_outputs)
    assert {os.path.relpath(source1), os.path.relpath(source2)} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_with_path_all_generation(runner, project):
    """Test that all generations are reported if only one of them is specified."""
    source = os.path.join(project, "source.txt")
    output1 = os.path.join(project, "data", "output1.txt")
    output2 = os.path.join(project, "data", "output2.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "--input", source, "touch", output1, output2])
    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(repo, source, "new content")

    result = get_status_command().build().execute(paths=[output1]).output

    assert ("data/output1.txt", {"source.txt"}) in result.outdated_outputs.items()
    assert ("data/output2.txt", {"source.txt"}) in result.outdated_outputs.items()
    assert 2 == len(result.outdated_outputs)
    assert {"source.txt"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_works_in_dirty_repository(runner, client):
    """Test status doesn't need a clean project and doesn't change anything."""
    source = project_properties.path / "source"
    write_and_commit_file(client.repository, source, "source content")
    assert 0 == runner.invoke(cli, ["run", "head", source], stdout="output").exit_code

    commit_sha_before = client.repository.head.commit.hexsha

    source.write_text("modified content")
    (project_properties.path / "untracked").write_text("untracked file")

    result = get_status_command().build().execute().output

    assert ("output", {"source"}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {"source"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)
    assert commit_sha_before == client.repository.head.commit.hexsha
    assert client.repository.untracked_files == ["untracked"]
    assert {c.a_path for c in client.repository.unstaged_changes} == {"source"}


def test_status_ignore_deleted_files(runner, client):
    """Test status can ignore deleted files."""
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "head", "source"], stdout="upstream").exit_code
    assert 0 == runner.invoke(cli, ["run", "tail", "upstream"], stdout="deleted").exit_code

    write_and_commit_file(client.repository, "source", "changes")
    Path("deleted").unlink()

    result = get_status_command().build().execute(ignore_deleted=True).output

    assert ("upstream", {"source"}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {"source"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_ignore_deleted_files_config(runner, client):
    """Test status can ignore deleted files when proper config is set."""
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "head", "source"], stdout="upstream").exit_code
    assert 0 == runner.invoke(cli, ["run", "tail", "upstream"], stdout="deleted").exit_code

    write_and_commit_file(client.repository, "source", "changes")
    Path("deleted").unlink()
    # Set config to ignore deleted files
    client.set_value("renku", "update_ignore_delete", "True")

    result = get_status_command().build().execute(ignore_deleted=False).output

    assert ("upstream", {"source"}) in result.outdated_outputs.items()
    assert 1 == len(result.outdated_outputs)
    assert {"source"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_deleted_files_reported_with_siblings(runner, client):
    """Test status reports deleted file if they have existing siblings."""
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "--input", "source", "touch", "deleted", "sibling"]).exit_code

    write_and_commit_file(client.repository, "source", "changes")
    Path("deleted").unlink()

    result = get_status_command().build().execute(ignore_deleted=True).output

    assert ("deleted", {"source"}) in result.outdated_outputs.items()
    assert ("sibling", {"source"}) in result.outdated_outputs.items()
    assert 2 == len(result.outdated_outputs)
    assert {"source"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert 0 == len(result.deleted_inputs)


def test_status_deleted_files_reported_with_downstream(runner, client):
    """Test status reports deleted file if they have existing downstreams."""
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "head", "source"], stdout="deleted").exit_code
    assert 0 == runner.invoke(cli, ["run", "tail", "deleted"], stdout="downstream").exit_code

    write_and_commit_file(client.repository, "source", "changes")
    Path("deleted").unlink()

    result = get_status_command().build().execute(ignore_deleted=True).output

    assert ("deleted", {"source"}) in result.outdated_outputs.items()
    assert ("downstream", {"source"}) in result.outdated_outputs.items()
    assert 2 == len(result.outdated_outputs)
    assert {"source"} == result.modified_inputs
    assert 0 == len(result.outdated_activities)
    assert {"deleted"} == result.deleted_inputs


def test_status_deleted_inputs(runner, project):
    """Test status when an input is deleted."""
    source = os.path.join(os.getcwd(), "source.txt")
    intermediate = os.path.join(os.getcwd(), "intermediate.txt")
    output = os.path.join(os.getcwd(), "data", "output.txt")

    repo = Repository(project)

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "cp", source, intermediate])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["run", "cp", intermediate, output])
    assert 0 == result.exit_code, format_result_exception(result)

    os.unlink(source)

    result = get_status_command().build().execute().output

    assert 0 == len(result.outdated_outputs)
    assert 0 == len(result.modified_inputs)
    assert 0 == len(result.outdated_activities)
    assert {"source.txt"} == result.deleted_inputs
