# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
"""Renku activity management tests."""

from pathlib import Path

from renku.core.workflow.activity import revert_activity
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from renku.infrastructure.repository import Repository
from tests.utils import create_and_commit_files, create_dummy_activity, create_dummy_plan


def create_dummy_activities(repository: Repository):
    """Create activities for tests in this file."""
    # Create files so that they can be found by git
    create_and_commit_files(
        repository,
        ("latest-generated", "old-content"),
        ("old-generated", "old-content"),
        "input",
        "output",
        "to-be-deleted",
        "used",
    )
    create_and_commit_files(repository, ("latest-generated", "new-content"), ("old-generated", "new-content"))

    plan = create_dummy_plan(name="plan")
    to_be_deleted_plan = create_dummy_plan(name="to-be-deleted-plan")

    upstream = create_dummy_activity(plan=plan, generations=["input", ("latest-generated", "old-content")])
    activity = create_dummy_activity(
        plan=to_be_deleted_plan,
        usages=["input"],
        generations=[("latest-generated", "new-content"), ("old-generated", "old-content"), "to-be-deleted", "used"],
    )
    downstream = create_dummy_activity(plan=plan, usages=["used"], generations=["output"])
    other = create_dummy_activity(
        plan=to_be_deleted_plan, usages=["other"], generations=[("old-generated", "new-content")]
    )

    activity_gateway = ActivityGateway()
    activity_gateway.add(upstream)
    activity_gateway.add(activity)
    activity_gateway.add(downstream)
    activity_gateway.add(other)

    return upstream, activity, downstream, other


def test_revert(project_with_injection):
    """Test reverting an activity."""
    _, activity, _, _ = create_dummy_activities(project_with_injection.repository)

    revert_activity(activity_id=activity.id, delete_plan=False, force=True, metadata_only=False)

    # Usage isn't deleted
    assert Path("input").exists()
    # If activity is the latest generator, generation will be reverted to an older version
    assert "old-content" == Path("latest-generated").read_text()
    # If activity is not the latest generator, generation wont' change
    assert "new-content" == Path("old-generated").read_text()
    # If activity is the only generator, generation will be deleted
    assert not Path("to-be-deleted").exists()
    # If activity is the only generator but there are some users, generation won't change
    assert Path("used").exists()


def test_revert_metadata_only(project_with_injection):
    """Test reverting an activity without reverting its generations."""
    _, activity, _, _ = create_dummy_activities(project_with_injection.repository)

    revert_activity(activity_id=activity.id, delete_plan=False, force=True, metadata_only=True)

    assert Path("input").exists()
    assert "new-content" == Path("latest-generated").read_text()
    assert "new-content" == Path("old-generated").read_text()
    assert Path("to-be-deleted").exists()
    assert Path("used").exists()


def test_revert_and_delete_plan(project_with_injection):
    """Test reverting an activity and deleting its plan."""
    _, activity, _, other = create_dummy_activities(project_with_injection.repository)
    plan_gateway = PlanGateway()

    revert_activity(activity_id=activity.id, delete_plan=True, force=True, metadata_only=True)

    # Plan isn't deleted since it's used by another activity
    assert plan_gateway.get_by_name("to-be-deleted-plan").deleted is False

    revert_activity(activity_id=other.id, delete_plan=True, force=True, metadata_only=True)

    # Plan is deleted because no other active activity is using it
    assert plan_gateway.get_by_name("to-be-deleted-plan").deleted is True
