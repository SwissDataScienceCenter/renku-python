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
"""Test ``update`` command."""

import os
import time
from pathlib import Path

import pytest

from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.plugin.provider import available_workflow_providers
from renku.core.project.project_properties import project_properties
from renku.domain_model.workflow.plan import Plan
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from tests.utils import delete_and_commit_file, format_result_exception, write_and_commit_file


@pytest.mark.parametrize("provider", available_workflow_providers())
@pytest.mark.parametrize("skip_metadata_update", [True, False])
def test_update(runner, client, renku_cli, client_database_injection_manager, provider, skip_metadata_update):
    """Test output is updated when source changes."""
    source = os.path.join(project_properties.path, "source.txt")
    output = os.path.join(project_properties.path, "output.txt")

    write_and_commit_file(client.repository, source, "content")

    exit_code, previous_activity = renku_cli("run", "head", "-1", source, stdout=output)
    assert 0 == exit_code

    write_and_commit_file(client.repository, source, "changed content")

    cmd = ["update", "-p", provider, "--all"]
    if skip_metadata_update:
        cmd.append("--skip-metadata-update")
    exit_code, activity = renku_cli(*cmd)

    assert 0 == exit_code
    assert "changed content" == Path(output).read_text()

    if skip_metadata_update:
        assert activity is None
    else:
        plan = activity.association.plan
        assert previous_activity.association.plan.id == plan.id
        assert isinstance(plan, Plan)
        result = runner.invoke(cli, ["status"])
        assert 0 == result.exit_code, format_result_exception(result)

    with client_database_injection_manager(client):
        activity_gateway = ActivityGateway()
        activity_collections = activity_gateway.get_all_activity_collections()

        # NOTE: No ActivityCollection is created if update include only one activity
        assert [] == activity_collections

        if skip_metadata_update:
            assert len(activity_gateway.get_all_activities()) == 1
        else:
            assert len(activity_gateway.get_all_activities()) == 2

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
@pytest.mark.parametrize("skip_metadata_update", [True, False])
def test_update_multiple_steps(
    runner, client, renku_cli, client_database_injection_manager, provider, skip_metadata_update
):
    """Test update in a multi-step workflow."""
    source = os.path.join(project_properties.path, "source.txt")
    intermediate = os.path.join(project_properties.path, "intermediate.txt")
    output = os.path.join(project_properties.path, "output.txt")

    write_and_commit_file(client.repository, source, "content")

    exit_code, activity1 = renku_cli("run", "cp", source, intermediate)
    assert 0 == exit_code
    exit_code, activity2 = renku_cli("run", "cp", intermediate, output)
    assert 0 == exit_code

    write_and_commit_file(client.repository, source, "changed content")

    cmd = ["update", "-p", provider, "--all"]
    if skip_metadata_update:
        cmd.append("--skip-metadata-update")
    exit_code, activities = renku_cli(*cmd)

    assert 0 == exit_code
    assert "changed content" == Path(intermediate).read_text()
    assert "changed content" == Path(output).read_text()

    if skip_metadata_update:
        assert activities is None
    else:
        plans = [a.association.plan for a in activities]
        assert 2 == len(plans)
        assert isinstance(plans[0], Plan)
        assert isinstance(plans[1], Plan)
        assert {p.id for p in plans} == {activity1.association.plan.id, activity2.association.plan.id}
        result = runner.invoke(cli, ["status"])
        assert 0 == result.exit_code, format_result_exception(result)

    with client_database_injection_manager(client):
        activity_gateway = ActivityGateway()
        activity_collections = activity_gateway.get_all_activity_collections()

        all_activities = activity_gateway.get_all_activities()
        if skip_metadata_update:
            assert len(all_activities) == 2
        else:
            assert 1 == len(activity_collections)
            assert {a.id for a in activities} == {a.id for a in activity_collections[0].activities}
            assert len(all_activities) == 4


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_multiple_steps_with_path(runner, project, renku_cli, provider):
    """Test update in a multi-step workflow when a path is specified."""
    repo = Repository(project)
    source = os.path.join(project, "source.txt")
    intermediate = os.path.join(project, "intermediate.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, activity1 = renku_cli("run", "cp", source, intermediate)
    assert 0 == exit_code
    exit_code, _ = renku_cli("run", "cp", intermediate, output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, activity = renku_cli("update", "-p", provider, intermediate)

    assert 0 == exit_code
    plan = activity.association.plan
    assert isinstance(plan, Plan)
    assert plan.id == activity1.association.plan.id

    assert "changed content" == Path(intermediate).read_text()
    assert "content" == Path(output).read_text()

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code, format_result_exception(result)
    assert "output.txt: intermediate.txt" in result.output
    assert "source.txt" not in result.output


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_with_directory_paths(project, renku_cli, provider):
    """Test update when a directory path is specified."""
    repo = Repository(project)
    data = os.path.join(project, "data", "dataset", "my-data")
    Path(data).mkdir(parents=True, exist_ok=True)
    source = os.path.join(project, "source.txt")
    output = os.path.join(data, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, previous_activity = renku_cli("run", "head", "-1", source, stdout=output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, activity = renku_cli("update", "-p", provider, data)

    assert 0 == exit_code
    assert "changed content" == Path(output).read_text()
    plan = activity.association.plan
    assert previous_activity.association.plan.id == plan.id


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_multiple_updates(runner, project, renku_cli, provider):
    """Test multiple updates of the same source."""
    repo = Repository(project)
    source = os.path.join(project, "source.txt")
    intermediate = os.path.join(project, "intermediate.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    exit_code, activity1 = renku_cli("run", "cp", source, intermediate)
    assert 0 == exit_code
    exit_code, activity2 = renku_cli("run", "cp", intermediate, output)
    assert 0 == exit_code

    write_and_commit_file(repo, source, "changed content")

    exit_code, _ = renku_cli("update", "-p", provider, "--all")
    assert 0 == exit_code
    assert "changed content" == Path(intermediate).read_text()

    write_and_commit_file(repo, source, "more changed content")

    exit_code, activities = renku_cli("update", "-p", provider, "--all")

    assert 0 == exit_code
    plans = [a.association.plan for a in activities]
    assert 2 == len(plans)
    assert isinstance(plans[0], Plan)
    assert isinstance(plans[1], Plan)
    assert {p.id for p in plans} == {activity1.association.plan.id, activity2.association.plan.id}

    assert "more changed content" == Path(intermediate).read_text()
    assert "more changed content" == Path(output).read_text()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_workflow_without_outputs(runner, project, run, provider):
    """Test workflow without outputs."""
    repo = Repository(project)
    source = os.path.join(project, "source.txt")

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "cat", "--no-output", source])

    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(repo, source, "changes")

    assert 1 == runner.invoke(cli, ["status"]).exit_code

    assert 0 == run(args=["update", "-p", provider, "--all"])

    result = runner.invoke(cli, ["status"])

    # NOTE: Activity is updated or otherwise status would still return 1
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_siblings(project, run, no_lfs_warning, provider):
    """Test all generations of an activity are updated together."""
    repo = Repository(project)
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

    assert 0 == run(args=["update", "-p", provider, brother])

    for sibling in siblings:
        assert "changed content" == sibling.read_text()

    # Siblings kept together even when one is removed.
    repo.remove(brother)
    repo.commit("Brother removed")
    assert not os.path.exists(brother)

    write_and_commit_file(repo, parent, "more content")

    # Update should create the missing sibling
    assert 0 == run(args=["update", "-p", provider, "--all"])

    for sibling in siblings:
        assert "more content" == sibling.read_text()


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_siblings_in_output_directory(project, run, provider):
    """Files in output directory are linked or removed after update."""
    repo = Repository(project)
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

    assert 0 == run(args=["update", "-p", provider, "output"])

    check_files()


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_relative_path_for_directory_input(client, run, renku_cli, provider):
    """Test having a directory input generates relative path in CWL."""
    write_and_commit_file(client.repository, project_properties.path / DATA_DIR / "file1", "file1")

    assert 0 == run(args=["run", "ls", DATA_DIR], stdout="ls.data")

    write_and_commit_file(client.repository, project_properties.path / DATA_DIR / "file2", "file2")

    exit_code, activity = renku_cli("update", "-p", provider, "--all")

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert "data" == plan.inputs[0].default_value


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_no_args(runner, project, no_lfs_warning, provider):
    """Test calling update with no args raises ParameterError."""
    repo = Repository(project)
    source = os.path.join(project, "source.txt")
    output = os.path.join(project, "output.txt")

    write_and_commit_file(repo, source, "content")

    result = runner.invoke(cli, ["run", "cp", source, output])
    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(repo, source, "changed content")

    before_commit = repo.head.commit

    result = runner.invoke(cli, ["update", "-p", provider])

    assert 2 == result.exit_code
    assert "Either PATHS, --all/-a, or --dry-run/-n should be specified." in result.output

    assert before_commit == repo.head.commit


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_with_no_execution(project, runner, provider):
    """Test update when no workflow is executed."""
    repo = Repository(project)
    input = os.path.join(project, "data", "input.txt")
    write_and_commit_file(repo, input, "content")

    result = runner.invoke(cli, ["update", "-p", provider, input], catch_exceptions=False)

    assert 1 == result.exit_code


def test_update_overridden_output(project, renku_cli, runner):
    """Test a path where final output is overridden will be updated partially."""
    repo = Repository(project)
    a = os.path.join(project, "a")
    b = os.path.join(project, "b")
    c = os.path.join(project, "c")

    write_and_commit_file(repo, a, "content")

    assert 0 == runner.invoke(cli, ["run", "--name", "r1", "cp", a, b]).exit_code
    time.sleep(1)
    assert 0 == runner.invoke(cli, ["run", "--name", "r2", "cp", b, c]).exit_code
    time.sleep(1)
    assert 0 == renku_cli("run", "--name", "r3", "wc", a, stdout=c).exit_code

    write_and_commit_file(repo, a, "new content")

    result = runner.invoke(cli, ["update", "--dry-run"])

    assert 0 == result.exit_code
    assert "r1" in result.output
    assert "r2" not in result.output
    assert "r3" in result.output


def test_update_overridden_outputs_partially(project, renku_cli, runner):
    """Test a path where one of the final output is overridden will be updated completely but in proper order."""
    repo = Repository(project)
    a = os.path.join(project, "a")
    b = os.path.join(project, "b")
    c = os.path.join(project, "c")
    d = os.path.join(project, "d")

    write_and_commit_file(repo, a, "content")

    assert 0 == runner.invoke(cli, ["run", "--name", "r1", "cp", a, b]).exit_code
    time.sleep(1)
    assert 0 == renku_cli("run", "--name", "r2", "tee", c, d, stdin=b).exit_code
    time.sleep(1)
    assert 0 == renku_cli("run", "--name", "r3", "wc", a, stdout=c).exit_code

    write_and_commit_file(repo, a, "new content")

    result = runner.invoke(cli, ["update", "--dry-run"])

    assert 0 == result.exit_code
    assert "r1" in result.output
    assert "r2" in result.output
    assert "r3" in result.output
    assert result.output.find("r2") < result.output.find("r3")


def test_update_multiple_paths_common_output(project, renku_cli, runner):
    """Test multiple paths that generate the same output will be updated except the last overridden step."""
    repo = Repository(project)
    a = os.path.join(project, "a")
    b = os.path.join(project, "b")
    c = os.path.join(project, "c")
    d = os.path.join(project, "d")

    write_and_commit_file(repo, a, "content")

    assert 0 == runner.invoke(cli, ["run", "--name", "r1", "cp", a, b]).exit_code
    time.sleep(1)
    assert 0 == runner.invoke(cli, ["run", "--name", "r2", "cp", b, d]).exit_code
    time.sleep(1)
    assert 0 == runner.invoke(cli, ["run", "--name", "r3", "cp", a, c]).exit_code
    time.sleep(1)
    assert 0 == renku_cli("run", "--name", "r4", "wc", c, stdout=d).exit_code

    write_and_commit_file(repo, a, "new content")

    result = runner.invoke(cli, ["update", "--dry-run"])

    assert 0 == result.exit_code
    assert "r1" in result.output
    assert "r2" not in result.output
    assert "r3" in result.output
    assert "r4" in result.output


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_update_with_execute(runner, client, renku_cli, client_database_injection_manager, provider):
    """Test output is updated when source changes."""
    source1 = Path("source.txt")
    output1 = Path("output.txt")
    source2 = Path("source2.txt")
    output2 = Path("output2.txt")
    script = Path("cp.sh")

    write_and_commit_file(client.repository, source1, "content_a")
    write_and_commit_file(client.repository, source2, "content_b")
    write_and_commit_file(client.repository, script, "cp $1 $2")

    result = runner.invoke(cli, ["run", "--name", "test", "bash", str(script), str(source1), str(output1)])
    assert 0 == result.exit_code, format_result_exception(result)

    assert (
        0
        == renku_cli(
            "workflow", "execute", "-p", provider, "--set", f"input-2={source2}", "--set", f"output-3={output2}", "test"
        ).exit_code
    )

    assert "content_a" == (project_properties.path / output1).read_text()
    assert "content_b" == (project_properties.path / output2).read_text()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(client.repository, script, "cp $1 $2\necho 'modified' >> $2")

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == renku_cli("update", "-p", provider, "--all").exit_code

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    assert "content_amodified\n" == (project_properties.path / output1).read_text()
    assert "content_bmodified\n" == (project_properties.path / output2).read_text()

    write_and_commit_file(client.repository, script, "cp $1 $2\necho 'even more modified' >> $2")

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == renku_cli("update", "-p", provider, "--all").exit_code

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    assert "content_aeven more modified\n" == (project_properties.path / output1).read_text()
    assert "content_beven more modified\n" == (project_properties.path / output2).read_text()


def test_update_with_external_files(runner, client, directory_tree):
    """Test update commands that use external files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-dataset", directory_tree]).exit_code

    path = project_properties.path / "data" / "my-dataset" / "directory_tree" / "file1"

    assert 0 == runner.invoke(cli, ["run", "tail", path], stdout="output").exit_code

    (directory_tree / "file1").write_text("updated file1")

    assert 0 == runner.invoke(cli, ["dataset", "update", "--all", "--no-external"]).exit_code
    assert "updated file1" not in (project_properties.path / "output").read_text()

    assert 0 == runner.invoke(cli, ["dataset", "update", "--all"]).exit_code

    result = runner.invoke(cli, ["update", "--all"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "updated file1" in (project_properties.path / "output").read_text()


def test_update_ignore_deleted_files(runner, client):
    """Test update can ignore deleted files."""
    deleted = project_properties.path / "deleted"
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "--name", "run-1", "head", "source"], stdout="upstream").exit_code
    assert 0 == runner.invoke(cli, ["run", "--name", "run-2", "tail", "upstream"], stdout=deleted).exit_code

    write_and_commit_file(client.repository, "source", "changes")
    delete_and_commit_file(client.repository, deleted)

    result = runner.invoke(cli, ["update", "--dry-run", "--all", "--ignore-deleted"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "run-1" in result.output
    assert "run-2" not in result.output

    result = runner.invoke(cli, ["update", "--all", "--ignore-deleted"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not deleted.exists()


def test_update_ignore_deleted_files_config(runner, client):
    """Test update can ignore deleted files when proper config is set."""
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "--name", "run-1", "head", "source"], stdout="upstream").exit_code
    assert 0 == runner.invoke(cli, ["run", "--name", "run-2", "tail", "upstream"], stdout="deleted").exit_code

    write_and_commit_file(client.repository, "source", "changes")
    delete_and_commit_file(client.repository, "deleted")
    # Set config to ignore deleted files
    client.set_value("renku", "update_ignore_delete", "True")
    client.repository.add(all=True)
    client.repository.commit("Set config")

    result = runner.invoke(cli, ["update", "--all", "--dry-run", "--ignore-deleted"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "run-1" in result.output
    assert "run-2" not in result.output


def test_update_deleted_files_reported_with_siblings(runner, client):
    """Test update regenerates deleted file if they have existing siblings."""
    deleted = project_properties.path / "deleted"
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "--input", "source", "touch", deleted, "sibling"]).exit_code

    write_and_commit_file(client.repository, "source", "changes")
    delete_and_commit_file(client.repository, deleted)

    result = runner.invoke(cli, ["update", "--all", "--ignore-deleted"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert deleted.exists()


def test_update_deleted_files_reported_with_downstream(runner, client):
    """Test update reports deleted file if they have existing downstreams."""
    deleted = project_properties.path / "deleted"
    write_and_commit_file(client.repository, "source", "source content")
    assert 0 == runner.invoke(cli, ["run", "head", "source"], stdout=deleted).exit_code
    assert 0 == runner.invoke(cli, ["run", "tail", deleted], stdout="downstream").exit_code

    write_and_commit_file(client.repository, "source", "changes")
    delete_and_commit_file(client.repository, deleted)

    result = runner.invoke(cli, ["update", "--all", "--ignore-deleted"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert deleted.exists()
