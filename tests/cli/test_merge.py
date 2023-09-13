#
# Copyright 2019-2023 - Swiss Data Science Center (SDSC)
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
"""Test ``move`` command."""

from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from renku.infrastructure.gateway.project_gateway import ProjectGateway
from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_mergetool(runner, project, directory_tree, run_shell, with_injection):
    """Test that merge tool can merge renku metadata."""
    result = runner.invoke(cli, ["mergetool", "install"])

    assert 0 == result.exit_code, format_result_exception(result)

    # create a common dataset
    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "shared-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    # Create a common workflow
    output = run_shell('renku run --name "shared-workflow" echo "a unique string" > my_output_file')

    assert b"" == output[0]
    assert output[1] is None

    # switch to a new branch
    output = run_shell("git checkout -b remote-branch")

    assert b"Switched to a new branch 'remote-branch'\n" == output[0]
    assert output[1] is None

    # edit the dataset
    result = runner.invoke(cli, ["dataset", "edit", "-d", "remote description", "shared-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "remote-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    # Create a new workflow
    output = run_shell('renku run --name "remote-workflow" echo "a unique string" > remote_output_file')

    assert b"" == output[0]
    assert output[1] is None

    # Create a downstream workflow
    output = run_shell('renku run --name "remote-downstream-workflow" cp my_output_file my_remote_downstream')

    assert b"" == output[0]
    assert output[1] is None

    # Create another downstream workflow
    output = run_shell('renku run --name "remote-downstream-workflow2" cp remote_output_file my_remote_downstream2')

    assert b"" == output[0]
    assert output[1] is None

    # Edit the project metadata
    result = runner.invoke(cli, ["project", "edit", "-k", "remote"])

    assert 0 == result.exit_code, format_result_exception(result)

    # Switch back to master
    output = run_shell("git checkout master")

    assert b"Switched to branch 'master'\n" == output[0]
    assert output[1] is None

    # Add a new dataset
    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "local-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    # Create a local workflow
    output = run_shell('renku run --name "local-workflow" echo "a unique string" > local_output_file')

    assert b"" == output[0]
    assert output[1] is None

    # Create a local downstream workflow
    output = run_shell('renku run --name "local-downstream-workflow" cp my_output_file my_local_downstream')

    assert b"" == output[0]
    assert output[1] is None

    # Create another local downstream workflow
    output = run_shell('renku run --name "local-downstream-workflow2" cp local_output_file my_local_downstream2')

    assert b"" == output[0]
    assert output[1] is None

    # Edit the project in master as well
    result = runner.invoke(cli, ["project", "edit", "-k", "local"])

    assert 0 == result.exit_code, format_result_exception(result)

    # Merge branches
    output = run_shell("git merge --no-edit remote-branch")

    assert b"Auto-merging" in output[0]
    assert b"files changed" in output[0]
    assert output[1] is None

    # check the metadata for datasets and activities can be correctly loaded
    result = runner.invoke(cli, ["log"])

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        project_gateway = ProjectGateway()
        project = project_gateway.get_project()
        datasets_provenance = DatasetsProvenance()
        datasets = list(datasets_provenance.datasets)
        activity_gateway = ActivityGateway()
        activities = activity_gateway.get_all_activities()
        plan_gateway = PlanGateway()
        plans = plan_gateway.get_all_plans()

    assert set(project.keywords) == {"local", "remote"}
    assert len(datasets) == 3
    assert len(activities) == 7
    assert len(plans) == 7

    shared_dataset = next(d for d in datasets if d.slug == "shared-dataset")
    assert "remote description" == shared_dataset.description


def test_mergetool_workflow_conflict(runner, project, run_shell, with_injection):
    """Test that merge tool can merge conflicting workflows."""
    result = runner.invoke(cli, ["mergetool", "install"])

    assert 0 == result.exit_code, format_result_exception(result)

    output = run_shell('renku run --name "shared-workflow" echo "a unique string" > my_output_file')

    assert b"" == output[0]
    assert output[1] is None

    # Switch to a new branch and create some workflows
    output = run_shell("git checkout -b remote-branch")

    assert b"Switched to a new branch 'remote-branch'\n" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "remote-workflow" cp my_output_file out1')

    assert b"" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "common-name" cp my_output_file out2')

    assert b"" == output[0]
    assert output[1] is None

    with with_injection():
        plan_gateway = PlanGateway()
        remote_plans = plan_gateway.get_newest_plans_by_names()

    # Switch to master and create conflicting workflows
    output = run_shell("git checkout master")

    assert b"Switched to branch 'master'\n" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "local-workflow" cp my_output_file out1')

    assert b"" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "common-name" cp my_output_file out2')

    assert b"" == output[0]
    assert output[1] is None

    # Merge branches, selecting remote to resolve the conflict
    output = run_shell("echo 'r\n' | git merge --no-edit remote-branch")

    assert b"Auto-merging" in output[0]
    assert b"files changed" in output[0]
    assert b"Merge conflict detected" in output[0]
    assert output[1] is None

    # check that the metadata can still be correctly loaded
    result = runner.invoke(cli, ["log"])

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "visualize", "out2"])

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        activity_gateway = ActivityGateway()
        activities = activity_gateway.get_all_activities()
        plan_gateway = PlanGateway()
        plans = plan_gateway.get_newest_plans_by_names()

    assert len(activities) == 5
    assert len(plans) == 4

    new_common = next(p for p in plans.values() if p.name == "common-name")
    remote_common = next(p for p in remote_plans.values() if p.name == "common-name")
    assert new_common.id == remote_common.id

    # Check that the merged workflow can be executed
    result = runner.invoke(cli, ["workflow", "execute", "common-name"])
    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        activity_gateway = ActivityGateway()
        activities = activity_gateway.get_all_activities()
        plan_gateway = PlanGateway()
        plans = plan_gateway.get_newest_plans_by_names()

    assert len(activities) == 6
    assert len(plans) == 4


def test_mergetool_workflow_complex_conflict(runner, project, run_shell, with_injection):
    """Test that merge tool can merge complex conflicts in workflows."""
    result = runner.invoke(cli, ["mergetool", "install"])

    assert 0 == result.exit_code, format_result_exception(result)

    output = run_shell('renku run --name "shared-workflow" echo "a unique string" > my_output_file')

    assert b"" == output[0]
    assert output[1] is None

    # Switch to a new branch and create some workflows
    output = run_shell("git checkout -b remote-branch")

    assert b"Switched to a new branch 'remote-branch'\n" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "intermediate-workflow" cp my_output_file intermediate')

    assert b"" == output[0]

    output = run_shell('renku run --name "final-workflow" cp intermediate final')

    assert b"" == output[0]

    with with_injection():
        plan_gateway = PlanGateway()
        remote_plans = plan_gateway.get_newest_plans_by_names()

    # Switch to master and create conflicting workflows
    output = run_shell("git checkout master")

    assert b"Switched to branch 'master'\n" == output[0]
    assert output[1] is None

    output = run_shell('renku run --name "intermediate-workflow" cp my_output_file intermediate')

    assert b"" == output[0]

    output = run_shell('renku run --name "final-workflow" cp intermediate final')

    assert b"" == output[0]

    with with_injection():
        plan_gateway = PlanGateway()
        local_plans = plan_gateway.get_newest_plans_by_names()

    # Merge branches, selecting local, then remote to resolve the conflict
    output = run_shell("echo 'l\nr\n' | git merge --no-edit remote-branch")

    assert b"Auto-merging" in output[0]
    assert b"files changed" in output[0]
    assert b"Merge conflict detected" in output[0]
    assert output[1] is None

    # check that the metadata can still be correctly loaded
    result = runner.invoke(cli, ["log"])

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "visualize", "final"])

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        activity_gateway = ActivityGateway()
        activities = activity_gateway.get_all_activities()
        plan_gateway = PlanGateway()
        plans = plan_gateway.get_newest_plans_by_names()

    assert len(activities) == 5
    assert len(plans) == 3

    new_intermediate = next(p for p in plans.values() if p.name == "intermediate-workflow")
    remote_intermediate = next(p for p in remote_plans.values() if p.name == "intermediate-workflow")
    assert new_intermediate.id == remote_intermediate.id

    new_final = next(p for p in plans.values() if p.name == "final-workflow")
    local_final = next(p for p in local_plans.values() if p.name == "final-workflow")
    assert new_final.id == local_final.id

    # Check that update works
    result = runner.invoke(cli, ["workflow", "execute", "--set", "parameter-1='different value'", "shared-workflow"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["update", "--all"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check that rerun works
    output = run_shell("git reset --hard HEAD^^")

    assert b"Merge branch 'remote-branch'" in output[0]
    assert output[1] is None

    # Check that the merged workflow can be executed
    result = runner.invoke(cli, ["rerun", "final"])
    assert 0 == result.exit_code, format_result_exception(result)
