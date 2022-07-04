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
"""Renku plan management tests."""

import pytest

from renku.core import errors
from renku.core.workflow.plan import (
    get_activities,
    get_derivative_chain,
    get_initial_id,
    get_latest_plan,
    is_plan_removed,
    remove_plan,
)
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from tests.utils import create_dummy_activity, create_dummy_plan


def create_dummy_plans():
    """Create plans for tests in this file."""
    grand_parent = create_dummy_plan(name="plan")
    parent = grand_parent.derive()
    plan = parent.derive()
    child = plan.derive()
    grand_child = child.derive()
    unrelated = create_dummy_plan(name="unrelated")

    plan_gateway = PlanGateway()
    plan_gateway.add(grand_parent)
    plan_gateway.add(parent)
    plan_gateway.add(plan)
    plan_gateway.add(child)
    plan_gateway.add(grand_child)
    plan_gateway.add(unrelated)

    return grand_parent, parent, plan, child, grand_child, unrelated


def test_get_latest_plan(injected_client):
    """Test getting latest plan in a derivative chain."""
    grand_parent, parent, plan, child, grand_child, unrelated = create_dummy_plans()

    latest_plan = get_latest_plan(plan)

    assert latest_plan is grand_child
    assert get_latest_plan(grand_parent) is grand_child
    assert get_latest_plan(parent) is grand_child
    assert get_latest_plan(child) is grand_child
    assert get_latest_plan(grand_child) is grand_child

    assert get_latest_plan(unrelated) is unrelated


def test_plan_get_derivatives_chain(injected_client):
    """Test getting plans that have parent/child relation."""
    grand_parent, parent, plan, child, grand_child, unrelated = create_dummy_plans()

    relatives = set(get_derivative_chain(plan))

    assert relatives == {grand_parent, parent, plan, child, grand_child}
    assert relatives == set(get_derivative_chain(grand_parent))
    assert relatives == set(get_derivative_chain(parent))
    assert relatives == set(get_derivative_chain(child))
    assert relatives == set(get_derivative_chain(grand_child))

    assert {unrelated} == set(get_derivative_chain(unrelated))

    assert [] == list(get_derivative_chain(None))


def test_plan_remove(injected_client):
    """Test deleting a plan."""
    grand_parent, parent, plan, child, grand_child, unrelated = create_dummy_plans()

    # Remove by name
    remove_plan(name_or_id=plan.name, force=True)

    # NOTE: We don't delete the plan itself
    assert not grand_parent.deleted
    assert not parent.deleted
    assert not plan.deleted
    assert not child.deleted
    assert not grand_child.deleted
    assert not unrelated.deleted

    last_derivative = get_latest_plan(plan)

    assert grand_child.id == last_derivative.derived_from
    assert last_derivative.deleted

    assert is_plan_removed(grand_parent)
    assert is_plan_removed(parent)
    assert is_plan_removed(plan)
    assert is_plan_removed(child)
    assert is_plan_removed(grand_child)
    assert not is_plan_removed(unrelated)


def test_plan_delete_errors(injected_client):
    """Test deleting a deleted plan or a non-existing plan."""
    _, _, plan, _, _, _ = create_dummy_plans()

    # Remove by id
    remove_plan(name_or_id=plan.id, force=True)

    # Remove a deleted plan
    with pytest.raises(errors.ParameterError, match="is already deleted"):
        remove_plan(name_or_id=plan.id, force=True)

    # Remove a non-existing plan
    with pytest.raises(errors.ParameterError, match="cannot be found"):
        remove_plan(name_or_id="non-existing", force=True)


def test_plan_get_initial_id(injected_client):
    """Test getting initial id of a plan."""
    grand_parent, parent, plan, child, grand_child, unrelated = create_dummy_plans()

    initial_id = get_initial_id(plan)

    assert initial_id == get_initial_id(grand_parent)
    assert initial_id == get_initial_id(parent)
    assert initial_id == get_initial_id(child)
    assert initial_id == get_initial_id(grand_child)


def test_get_activities(injected_client):
    """Test getting activities of a plan."""
    grand_parent, parent, plan, child, grand_child, unrelated = create_dummy_plans()
    activities = [
        create_dummy_activity(plan),
        create_dummy_activity(grand_parent),
        create_dummy_activity(grand_child),
        create_dummy_activity(child),
        create_dummy_activity(plan),
        create_dummy_activity(unrelated),
        create_dummy_activity("other-plan"),
    ]
    activity_gateway = ActivityGateway()
    for activity in activities:
        activity_gateway.add(activity)

    plan_activities = set(get_activities(plan))

    assert set(activities[0:5]) == plan_activities
