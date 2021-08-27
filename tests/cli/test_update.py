# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test ``update`` command."""

import os
from pathlib import Path

import git

from renku.cli import cli
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.workflow.composite_plan import PlanCollection
from renku.core.models.workflow.plan import Plan
from tests.utils import format_result_exception, write_and_commit_file


def test_update(runner, project, renku_cli):
    """Test output is updated when source changes."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, previous_activity = renku_cli("run", "head", "-1", source, stdout=output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, activity = renku_cli("update", "--all")

    assert 0 == exit_code
    plan = activity.association.plan
    assert previous_activity.association.plan.id == plan.id
    assert isinstance(plan, Plan)
    assert not isinstance(plan, PlanCollection)

    assert "changed content" == Path(output).read_text()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_update_multiple_steps(runner, project, renku_cli):
    """Test update in a multi-step workflow."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")
    intermediate = os.path.join(project, "intermediate.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, activity1 = renku_cli("run", "cp", source, intermediate)
    assert 0 == exit_code
    exit_code, activity2 = renku_cli("run", "cp", intermediate, output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, activity = renku_cli("update", "--all")

    assert 0 == exit_code
    plan = activity.association.plan
    # assert previous_activity.association.plan.id == plan.id
    assert not isinstance(plan, Plan)
    assert isinstance(plan, PlanCollection)
    assert {p.id for p in plan.plans} == {activity1.association.plan.id, activity2.association.plan.id}

    assert "changed content" == Path(intermediate).read_text()
    assert "changed content" == Path(output).read_text()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_update_multiple_steps_with_path(runner, project, renku_cli):
    """Test update in a multi-step workflow when a path is specified."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")
    intermediate = os.path.join(project, "intermediate.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, activity1 = renku_cli("run", "cp", source, intermediate)
    assert 0 == exit_code
    exit_code, activity2 = renku_cli("run", "cp", intermediate, output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, activity = renku_cli("update", intermediate)

    assert 0 == exit_code
    plan = activity.association.plan
    # assert previous_activity.association.plan.id == plan.id
    assert isinstance(plan, Plan)
    assert not isinstance(plan, PlanCollection)
    assert plan.id == activity1.association.plan.id

    assert "changed content" == Path(intermediate).read_text()
    assert "content" == Path(output).read_text()

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code, format_result_exception(result)
    assert "output.txt: intermediate.txt" in result.output
    assert "source.txt" not in result.output


def test_update_workflow_without_outputs(runner, project, run):
    """Test workflow without outputs."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")

    write_and_commit_file(repo, source, "content")

    assert 0 == runner.invoke(cli, ["run", "cat", "--no-output", source]).exit_code

    write_and_commit_file(repo, source, "changes")

    assert 1 == runner.invoke(cli, ["status"]).exit_code

    assert 0 == run(args=["update", "--all"])

    result = runner.invoke(cli, ["status"])

    # NOTE: Activity is updated or otherwise status would still return 1
    assert 0 == result.exit_code, format_result_exception(result)


def test_update_siblings(project, run, no_lfs_warning):
    """Test all generations of an activity are updated together."""
    repo = git.Repo(project)
    parent = os.path.join(project, "parent.txt")
    brother = os.path.join(project, "brother.txt")
    sister = os.path.join(project, "sister.txt")
    siblings = [Path(brother), Path(sister)]

    write_and_commit_file(repo, parent, "content")

    assert 0 == run(args=["run", "tee", brother, sister], stdin=parent)

    # The output file is copied from the source.
    for sibling in siblings:
        assert "content" == sibling.read_text()

    write_and_commit_file(repo, parent, "changed content")

    assert 0 == run(args=["update", brother])

    for sibling in siblings:
        assert "changed content" == sibling.read_text()

    # Siblings kept together even when one is removed.
    repo.index.remove([brother], working_tree=True)
    repo.index.commit("Brother removed")
    assert not os.path.exists(brother)

    write_and_commit_file(repo, parent, "more content")

    # Update should create the missing sibling
    assert 0 == run(args=["update", "--all"])

    for sibling in siblings:
        assert "more content" == sibling.read_text()


def test_update_siblings_in_output_directory(project, run):
    """Files in output directory are linked or removed after update."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")
    output = Path(os.path.join(project, "output"))  # a directory

    def write_source():
        """Write source from files."""
        write_and_commit_file(repo, source, content="\n".join(" ".join(line) for line in files) + "\n")

    def check_files():
        """Check file content."""
        assert len(files) == len(list(output.rglob("*")))

        for name, content in files:
            assert content == (output / name).read_text().strip()

    files = [("first", "1"), ("second", "2"), ("third", "3")]
    write_source()

    script = 'mkdir -p "$0"; ' "cat - | while read -r name content; do " 'echo "$content" > "$0/$name"; done'

    assert not os.path.exists(output)

    assert 0 == run(args=["run", "sh", "-c", script, "output"], stdin=source)

    assert os.path.exists(output)
    check_files()

    files = [("third", "3"), ("fourth", "4")]
    write_source()

    assert 0 == run(args=["update", "output"])

    check_files()


def test_update_relative_path_for_directory_input(client, run, renku_cli):
    """Test having a directory input generates relative path in CWL."""
    write_and_commit_file(client.repo, client.path / DATA_DIR / "file1", "file1")

    assert 0 == run(args=["run", "ls", DATA_DIR], stdout="ls.data")

    write_and_commit_file(client.repo, client.path / DATA_DIR / "file2", "file2")

    exit_code, activity = renku_cli("update", "--all")

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert "data" == plan.inputs[0].default_value


def test_update_no_args(runner, project, no_lfs_warning):
    """Test calling update with no args raises ParameterError."""
    repo = git.Repo(project)
    source = os.path.join(project, "source.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    assert 0 == runner.invoke(cli, ["run", "cp", source, output]).exit_code

    write_and_commit_file(repo, source, "changed content")

    before_commit = repo.head.commit

    result = runner.invoke(cli, ["update"])

    assert 2 == result.exit_code
    assert "Either PATHS or --all/-a should be specified" in result.output

    assert before_commit == repo.head.commit
