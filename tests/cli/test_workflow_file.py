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
"""Tests for workflow file."""

import textwrap
import time
from string import Template

import pytest

from renku.core.plugin.provider import available_workflow_providers
from renku.core.plugin.workflow_file_parser import read_workflow_file
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from renku.ui.cli import cli
from tests.cli.fixtures.cli_projects import RenkuWorkflowFileProject
from tests.utils import format_result_exception


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_run_workflow_file_with_provider(runner, workflow_file_project, provider):
    """Test running a workflow file."""
    commit_before = workflow_file_project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["run", "--provider", provider, workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    # A commit is created
    assert commit_before != workflow_file_project.repository.head.commit.hexsha

    assert (workflow_file_project.path / "results" / "output.csv").exists()
    assert (workflow_file_project.path / "results" / "output.csv.wc").exists()

    output_text = (workflow_file_project.path / "results" / "output.csv.wc").read_text()
    assert "5 " in output_text
    assert "results/output.csv" in output_text


def test_dry_run_workflow_file(runner, workflow_file_project):
    """Test running a workflow file with ``--dry-run``."""
    commit_before = workflow_file_project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["run", "--dry-run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    # No commit is created
    assert commit_before == workflow_file_project.repository.head.commit.hexsha

    # No output is created
    assert not (workflow_file_project.path / "results" / "output.csv").exists()
    assert not (workflow_file_project.path / "results" / "output.csv.wc").exists()

    assert "Will execute step 'head': head $n $models $colors > $temporary-result" in result.output
    assert "Will execute step 'tail': tail $parameters intermediate > results/output.csv" in result.output
    assert "Will execute step 'line-count': wc -l $models-and-colors > $output" in result.output


def test_run_workflow_file_with_selected_steps(runner, workflow_file_project):
    """Test running a sub-set of steps of a workflow file."""
    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file, "head", "tail"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Executing step 'workflow-file.head':" in result.output
    assert "Executing step 'workflow-file.tail':" in result.output
    assert "Executing step 'workflow-file.line-count':" not in result.output

    # Third step's output isn't created
    assert not (workflow_file_project.path / "results" / "output.csv.wc").exists()


def test_run_non_existing_workflow_file(runner, workflow_file_project):
    """Test running a non-existing workflow file gives proper error if file has YAML extension."""
    result = runner.invoke(cli, ["run", "non-existing-workflow-file.yml"])

    assert 2 == result.exit_code, format_result_exception(result)

    assert "No such file or directory: 'non-existing-workflow-file.yml'" in result.output


def test_run_workflow_file_with_no_commit(runner, workflow_file_project):
    """Test running a workflow file with ``--no-commit`` option."""
    commit_before = workflow_file_project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["run", "--no-commit", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    # No commit is created
    assert commit_before == workflow_file_project.repository.head.commit.hexsha

    assert (workflow_file_project.path / "results" / "output.csv").exists()
    assert (workflow_file_project.path / "results" / "output.csv.wc").exists()

    output_text = (workflow_file_project.path / "results" / "output.csv.wc").read_text()
    assert "5 " in output_text
    assert "results/output.csv" in output_text

    # NOTE: No renku metadata is persisted
    assert {
        "workflow-file.yml",
        "data/collection/colors.csv",
        "data/collection/models.csv",
        "intermediate",
        "results/output.csv",
        "results/output.csv.wc",
    } == set(workflow_file_project.repository.untracked_files)


@pytest.mark.parametrize("workflow_file_project", ["workflow-file-simple.yml"], indirect=True)
def test_run_workflow_file_in_dirty_repository(runner, workflow_file_project: RenkuWorkflowFileProject):
    """Test running a workflow file in a dirty repository doesn't commit files that are not part of workflow file."""
    dirty_files = {"Dockerfile"}
    part_of_workflow_file = {"workflow-file-simple.yml", "data/collection/colors.csv", "data/collection/models.csv"}
    untracked_files = {"untracked"}
    staged_files = {"staged"}

    (workflow_file_project.path / "Dockerfile").write_text("some changes")
    (workflow_file_project.path / "untracked").touch()
    (workflow_file_project.path / "staged").touch()
    workflow_file_project.repository.add(workflow_file_project.path / "staged")
    assert workflow_file_project.repository.is_dirty()
    assert dirty_files == {c.a_path for c in workflow_file_project.repository.unstaged_changes}
    assert untracked_files | part_of_workflow_file == set(workflow_file_project.repository.untracked_files)
    assert staged_files == {c.a_path for c in workflow_file_project.repository.staged_changes}

    commit_before = workflow_file_project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    # A commit is created
    assert commit_before != workflow_file_project.repository.head.commit.hexsha

    # Repository is still dirty with the same files as before
    assert workflow_file_project.repository.is_dirty()
    assert dirty_files == {c.a_path for c in workflow_file_project.repository.unstaged_changes}
    # Untracked files that were parts of the workflow file are committed and the rest remained untracked
    assert untracked_files == set(workflow_file_project.repository.untracked_files)
    assert staged_files == {c.a_path for c in workflow_file_project.repository.staged_changes}


def test_workflow_file_with_no_persist(runner, workflow_file_project: RenkuWorkflowFileProject):
    """Test generated outputs that have ``persist == False`` aren't committed."""
    assert "intermediate" not in workflow_file_project.repository.untracked_files

    commit_before = workflow_file_project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    # A commit is created
    assert commit_before != workflow_file_project.repository.head.commit.hexsha

    # Output ``intermediate`` is not committed
    assert "intermediate" in workflow_file_project.repository.untracked_files


def test_export_graph_with_workflow_file(runner, workflow_file_project):
    """Test exporting KG graph includes metadata for workflow files."""
    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--full", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "workflow-file" in result.output
    assert "workflow-file.head" in result.output
    assert "workflow-file.tail" in result.output
    assert "workflow-file.line-count" in result.output
    assert "renku-ontology#WorkflowFileActivityCollection" in result.output
    assert "renku-ontology#WorkflowFileCompositePlan" in result.output
    assert "renku-ontology#WorkflowFilePlan" in result.output


def test_show_a_workflow_file(runner, workflow_file_project):
    """Test show a workflow file."""
    result = runner.invoke(cli, ["workflow", "show", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Name: workflow-file" in result.output
    assert "Path: workflow-file.yml" in result.output
    assert "Keywords: workflow file, v1" in result.output
    assert "Description: A sample workflow file used for testing" in result.output
    assert " head:" in result.output
    assert " tail:" in result.output
    assert " line-count:" in result.output
    assert "Command: head $n $models $colors > $temporary-result" in result.output
    assert "Processed command: head" in result.output
    assert "Keywords: preprocessing, first step" in result.output
    assert "Description: first stage of the pipeline" in result.output
    assert " models:" in result.output
    assert "Path: data/collection/models.csv" in result.output
    assert "Position: 2" in result.output
    assert "Description: all available model numbers" in result.output
    assert " temporary-result:" in result.output
    assert "Mapped to: stdout" in result.output
    assert " n:" in result.output
    assert "Value: 10" in result.output
    assert "Prefix: -n" in result.output


def test_export_plan_to_workflow_file(runner, project):
    """Test exporting a plan to a workflow file."""
    result = runner.invoke(cli, ["run", "--name", "r1", "head", "-n", "20", "Dockerfile"], stdout="intermediate")
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "export", "--format", "renku", "r1"], stdout="workflow-file.yml")
    assert 0 == result.exit_code, format_result_exception(result)

    workflow_file = read_workflow_file(project.path / "workflow-file.yml")

    assert "r1-workflow-file" == workflow_file.name

    assert "r1" == workflow_file.steps[0].name
    assert "input-2" == workflow_file.steps[0].inputs[0].name
    assert "Dockerfile" == workflow_file.steps[0].inputs[0].path
    assert workflow_file.steps[0].inputs[0].mapped_to is None
    assert "output-3" == workflow_file.steps[0].outputs[0].name
    assert "intermediate" == workflow_file.steps[0].outputs[0].path
    assert "stdout" == workflow_file.steps[0].outputs[0].mapped_to
    assert "n-1" == workflow_file.steps[0].parameters[0].name
    assert "-n " == workflow_file.steps[0].parameters[0].prefix
    assert "20" == workflow_file.steps[0].parameters[0].value


def test_export_composite_plan_to_workflow_file(runner, project):
    """Test exporting a composite plan to a workflow file."""
    result = runner.invoke(cli, ["run", "--name", "r1", "head", "-n", "20", "Dockerfile"], stdout="intermediate")
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["run", "--name", "r2", "tail", "-n", "10", "intermediate"], stdout="output")
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["workflow", "compose", "r1r2", "r1", "r2"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "export", "--format", "renku", "r1r2"], stdout="workflow-file.yml")
    assert 0 == result.exit_code, format_result_exception(result)

    workflow_file = read_workflow_file(project.path / "workflow-file.yml")

    assert "r1r2" == workflow_file.name
    assert ["r1", "r2"] == [s.name for s in workflow_file.steps]

    assert "input-2" == workflow_file.steps[1].inputs[0].name
    assert "Dockerfile" == workflow_file.steps[0].inputs[0].path
    assert "output-3" == workflow_file.steps[0].outputs[0].name
    assert "intermediate" == workflow_file.steps[0].outputs[0].path
    assert "stdout" == workflow_file.steps[0].outputs[0].mapped_to
    assert "n-1" == workflow_file.steps[0].parameters[0].name
    assert "-n " == workflow_file.steps[0].parameters[0].prefix
    assert "20" == workflow_file.steps[0].parameters[0].value

    assert "input-2" == workflow_file.steps[1].inputs[0].name
    assert "intermediate" == workflow_file.steps[1].inputs[0].path
    assert "output-3" == workflow_file.steps[1].outputs[0].name
    assert "output" == workflow_file.steps[1].outputs[0].path
    assert "stdout" == workflow_file.steps[1].outputs[0].mapped_to
    assert "n-1" == workflow_file.steps[1].parameters[0].name
    assert "-n " == workflow_file.steps[1].parameters[0].prefix
    assert "10" == workflow_file.steps[1].parameters[0].value


def test_workflow_file_plan_versioning(runner, workflow_file_project, with_injection):
    """Test plans in a workflow file are versioned."""
    workflow_file = workflow_file_project.path / "workflow-file.yml"
    content = Template(
        textwrap.dedent(
            """
            name: workflow-file
            steps:
              head:
                command: head $$parameters $$inputs > $$outputs
                inputs:
                  - data/collection/models.csv
                outputs:
                  - intermediate
                parameters:
                  n:
                    prefix: -n
                    value: $n
              tail:
                command: tail $$parameters $$inputs > $$outputs
                inputs:
                  - intermediate
                outputs:
                  - output.csv
                parameters:
                  n:
                    prefix: -n
                    value: 5
              line-count:
                command: wc $$parameters $$inputs > $$outputs
                inputs:
                  - output.csv
                outputs:
                  - output.csv.wc
            """
        )
    )

    workflow_file.write_text(content.safe_substitute(n=10))
    result = runner.invoke(cli, ["run", workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)
    time.sleep(1)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_1 = plan_gateway.get_by_name("workflow-file")
        head_1 = plan_gateway.get_by_name("workflow-file.head")
        tail_1 = plan_gateway.get_by_name("workflow-file.tail")
        line_count_1 = plan_gateway.get_by_name("workflow-file.line-count")

    workflow_file.write_text(content.safe_substitute(n=20))
    result = runner.invoke(cli, ["run", workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)
    time.sleep(1)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_2 = plan_gateway.get_by_name("workflow-file")
        head_2 = plan_gateway.get_by_name("workflow-file.head")
        tail_2 = plan_gateway.get_by_name("workflow-file.tail")
        line_count_2 = plan_gateway.get_by_name("workflow-file.line-count")

    workflow_file.write_text(content.safe_substitute(n=30))
    result = runner.invoke(cli, ["run", workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_3 = plan_gateway.get_by_name("workflow-file")
        head_3 = plan_gateway.get_by_name("workflow-file.head")
        tail_3 = plan_gateway.get_by_name("workflow-file.tail")
        line_count_3 = plan_gateway.get_by_name("workflow-file.line-count")

    # Root plan has newer versions since one of its sub-plans changed
    assert root_plan_2.id != root_plan_1.id
    assert root_plan_2.derived_from == root_plan_1.id
    assert root_plan_3.id != root_plan_2.id
    assert root_plan_3.derived_from == root_plan_2.id

    # ``head`` has newer versions
    assert head_2.id != head_1.id
    assert head_2.derived_from == head_1.id
    assert head_3.id != head_2.id
    assert head_3.derived_from == head_2.id

    # ``tail`` and ``line-count`` don't change and won't have newer versions
    assert tail_1.id == tail_2.id == tail_3.id
    assert tail_1.derived_from is None
    assert tail_2.derived_from is None
    assert tail_3.derived_from is None

    assert line_count_1.id == line_count_2.id == line_count_3.id
    assert line_count_1.derived_from is None
    assert line_count_2.derived_from is None
    assert line_count_3.derived_from is None


def test_workflow_file_plan_versioning_with_selected_steps(runner, workflow_file_project, with_injection):
    """Test plans are versioned correctly when executing subsets of steps."""
    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file, "head", "tail"])
    assert 0 == result.exit_code, format_result_exception(result)
    time.sleep(1)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_1 = plan_gateway.get_by_name("workflow-file")
        head_1 = plan_gateway.get_by_name("workflow-file.head")
        tail_1 = plan_gateway.get_by_name("workflow-file.tail")
        line_count_1 = plan_gateway.get_by_name("workflow-file.line-count")

    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    time.sleep(1)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_2 = plan_gateway.get_by_name("workflow-file")
        head_2 = plan_gateway.get_by_name("workflow-file.head")
        tail_2 = plan_gateway.get_by_name("workflow-file.tail")
        line_count_2 = plan_gateway.get_by_name("workflow-file.line-count")

    # Plan `line-count` wasn't executed in the first run
    assert line_count_1 is None
    assert line_count_2 is not None

    # Everything else is the same
    assert root_plan_2.id == root_plan_1.id
    assert head_2.id == head_1.id
    assert tail_2.id == tail_1.id


def test_duplicate_workflow_file_plan_name(runner, workflow_file_project):
    """Test workflow file execution fails if a plan with the same name exists."""
    workflow_file_project.repository.add(all=True)
    workflow_file_project.repository.commit("Commit before run")

    result = runner.invoke(cli, ["run", "--name", "workflow-file", "echo", "hello world!"], stdout="output")
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "Duplicate workflow file name: Workflow 'workflow-file' already exists." in result.output

    # Showing workflow files with duplicate name still works
    result = runner.invoke(cli, ["workflow", "show", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_file_plan_versioning_when_moved(runner, workflow_file_project, with_injection):
    """Test workflow file steps will be new plans when moved.

    NOTE: Plans are versioned based on their path and name only. Moving a workflow file creates new plans even if they
    haven't changed.
    """
    workflow_file = workflow_file_project.path / "workflow-file.yml"
    content = Template(
        textwrap.dedent(
            """
            name: $name
            steps:
              head:
                command: head $$parameters $$inputs > $$outputs
                inputs:
                  - data/collection/models.csv
                outputs:
                  - intermediate
                parameters:
                  n:
                    prefix: -n
                    value: 10
              tail:
                command: tail $$parameters $$inputs > $$outputs
                inputs:
                  - intermediate
                outputs:
                  - output.csv
                parameters:
                  n:
                    prefix: -n
                    value: 5
            """
        )
    )

    workflow_file.write_text(content.safe_substitute(name="workflow-file"))
    result = runner.invoke(cli, ["run", workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)
    time.sleep(1)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_1 = plan_gateway.get_by_name("workflow-file")
        head_1 = plan_gateway.get_by_name("workflow-file.head")
        tail_1 = plan_gateway.get_by_name("workflow-file.tail")

    moved_workflow_file = workflow_file_project.path / "moved-workflow-file.yml"
    moved_workflow_file.write_text(content.safe_substitute(name="moved-workflow-file"))
    result = runner.invoke(cli, ["run", moved_workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        plan_gateway = PlanGateway()
        root_plan_2 = plan_gateway.get_by_name("moved-workflow-file")
        head_2 = plan_gateway.get_by_name("moved-workflow-file.head")
        tail_2 = plan_gateway.get_by_name("moved-workflow-file.tail")

    assert root_plan_2.id != root_plan_1.id
    assert root_plan_2.derived_from is None

    # ``head`` and ``tail`` are new plans
    assert head_2.id != head_1.id
    assert head_2.derived_from is None

    assert tail_1.id != tail_2.id
    assert tail_2.derived_from is None


def test_workflow_file_is_visualized_as_dependency(runner, workflow_file_project):
    """Test visualizing a path that is generated by a workflow file, shows the workflow file as a dependency."""
    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "visualize", "results/output.csv.wc"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "workflow-file.yml" in result.output


def test_workflow_file_status(runner, workflow_file_project):
    """Test ``renku status`` shows if a workflow file has changes."""
    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)
    time.sleep(1)

    workflow_file = workflow_file_project.path / workflow_file_project.workflow_file
    workflow_file.write_text(workflow_file.read_text() + "\n")

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Outdated workflow files and their outputs(1):" in result.output
    assert "workflow-file.yml: intermediate, results/output.csv, results/output.csv.wc" in result.output

    result = runner.invoke(cli, ["run", workflow_file_project.workflow_file])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Outdated workflow files and their outputs(1):" not in result.output
    assert "Everything is up-to-date" in result.output
