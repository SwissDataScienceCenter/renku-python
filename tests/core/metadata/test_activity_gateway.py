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
"""Test activity database gateways."""

import pytest

from renku.core import errors
from renku.domain_model.workflow.plan import Plan
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from tests.utils import create_dummy_activity


def test_get_by_id(project_with_injection):
    """Testing getting an activity by its id."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="")

    activity_1 = create_dummy_activity(plan=plan, id="/activities/activity-1")
    activity_2 = create_dummy_activity(plan=plan, id="/activities/activity-2")
    activity_3 = create_dummy_activity(plan=plan, id="/activities/activity-3")

    activity_gateway = ActivityGateway()

    activity_gateway.add(activity_1)
    activity_gateway.add(activity_2)
    activity_gateway.add(activity_3)

    activity = activity_gateway.get_by_id("/activities/activity-2")

    assert activity is activity_2


def test_get_by_id_non_existing(project_with_injection):
    """Testing getting a non-existing activity id returns None."""
    activity_gateway = ActivityGateway()

    activity = activity_gateway.get_by_id("/activities/non-existing-id")

    assert activity is None


def test_activity_gateway_downstream_activities(project_with_injection):
    """Test getting downstream activities work."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="")

    intermediate = create_dummy_activity(plan=plan, usages=["some/data"], generations=["other/data/file"])
    previous = create_dummy_activity(plan=plan, generations=["some/"])
    following = create_dummy_activity(plan=plan, usages=["other/data"])
    unrelated = create_dummy_activity(plan=plan, usages=["unrelated_in"], generations=["unrelated_out"])

    activity_gateway = ActivityGateway()

    activity_gateway.add(intermediate)
    activity_gateway.add(following)
    activity_gateway.add(previous)
    activity_gateway.add(unrelated)

    downstream = activity_gateway.get_downstream_activities(following)

    assert not downstream

    downstream = activity_gateway.get_downstream_activities(intermediate)
    assert {following.id} == {a.id for a in downstream}

    downstream = activity_gateway.get_downstream_activities(previous)
    assert {following.id, intermediate.id} == {a.id for a in downstream}


def test_activity_gateway_upstream_activities(project_with_injection):
    """Test getting upstream activities work."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="")

    previous = create_dummy_activity(plan=plan, generations=["some/"])
    intermediate = create_dummy_activity(plan=plan, usages=["some/data"], generations=["other/data/file"])
    following = create_dummy_activity(plan=plan, usages=["other/data"])
    unrelated = create_dummy_activity(plan=plan, usages=["unrelated_in"], generations=["unrelated_out"])

    activity_gateway = ActivityGateway()

    activity_gateway.add(previous)
    activity_gateway.add(intermediate)
    activity_gateway.add(following)
    activity_gateway.add(unrelated)

    upstream = activity_gateway.get_upstream_activities(previous)

    assert not upstream

    upstream = activity_gateway.get_upstream_activities(intermediate)
    assert {previous.id} == {a.id for a in upstream}

    upstream = activity_gateway.get_upstream_activities(following)
    assert {previous.id, intermediate.id} == {a.id for a in upstream}


def test_activity_gateway_downstream_activity_chains(project_with_injection):
    """Test getting downstream activity chains work."""
    r1 = create_dummy_activity(plan="r1", usages=["a"], generations=["b"])
    r2 = create_dummy_activity(plan="r2", usages=["b"], generations=["c"])
    r3 = create_dummy_activity(plan="r3", usages=["d"], generations=["e"])
    r4 = create_dummy_activity(plan="r4", usages=["c", "e"], generations=["f", "g"])
    r5 = create_dummy_activity(plan="r5", usages=["f"], generations=["h"])
    r6 = create_dummy_activity(plan="r6", usages=["g"], generations=["i"])
    r7 = create_dummy_activity(plan="r7", usages=["x"], generations=["y"])

    activity_gateway = ActivityGateway()

    activity_gateway.add(r1)
    activity_gateway.add(r3)
    activity_gateway.add(r2)
    activity_gateway.add(r4)
    activity_gateway.add(r5)
    activity_gateway.add(r6)
    activity_gateway.add(r7)

    assert [] == activity_gateway.get_downstream_activity_chains(r6)

    downstream_chains = activity_gateway.get_downstream_activity_chains(r1)
    assert {(r2.id,), (r2.id, r4.id), (r2.id, r4.id, r5.id), (r2.id, r4.id, r6.id)} == {
        tuple(a.id for a in chain) for chain in downstream_chains
    }

    downstream_chains = activity_gateway.get_downstream_activity_chains(r4)
    assert {(r5.id,), (r6.id,)} == {tuple(a.id for a in chain) for chain in downstream_chains}

    assert [] == activity_gateway.get_downstream_activity_chains(r7)


def test_activity_gateway_upstream_activity_chains(project_with_injection):
    """Test getting upstream activity chains work."""
    r1 = create_dummy_activity(plan="r1", usages=["a"], generations=["b"])
    r2 = create_dummy_activity(plan="r2", usages=["b"], generations=["c"])
    r3 = create_dummy_activity(plan="r3", usages=["d"], generations=["e"])
    r4 = create_dummy_activity(plan="r4", usages=["c", "e"], generations=["f", "g"])
    r5 = create_dummy_activity(plan="r5", usages=["f"], generations=["h"])
    r6 = create_dummy_activity(plan="r6", usages=["g"], generations=["i"])
    r7 = create_dummy_activity(plan="r7", usages=["x"], generations=["y"])

    activity_gateway = ActivityGateway()

    activity_gateway.add(r1)
    activity_gateway.add(r3)
    activity_gateway.add(r2)
    activity_gateway.add(r4)
    activity_gateway.add(r5)
    activity_gateway.add(r6)
    activity_gateway.add(r7)

    assert [] == activity_gateway.get_upstream_activity_chains(r1)

    downstream_chains = activity_gateway.get_upstream_activity_chains(r6)
    assert {(r4.id,), (r4.id, r3.id), (r4.id, r2.id), (r4.id, r2.id, r1.id)} == {
        tuple(a.id for a in chain) for chain in downstream_chains
    }

    downstream_chains = activity_gateway.get_upstream_activity_chains(r4)
    assert {(r3.id,), (r2.id,), (r2.id, r1.id)} == {tuple(a.id for a in chain) for chain in downstream_chains}

    assert [] == activity_gateway.get_upstream_activity_chains(r7)


def test_remove_activity(project_with_injection):
    """Test removing an activity."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="")

    upstream = create_dummy_activity(plan=plan, generations=["input"])
    activity = create_dummy_activity(plan=plan, usages=["input", "origin"], generations=["intermediate", "other"])
    downstream = create_dummy_activity(plan=plan, usages=["intermediate"], generations=["output"])
    other = create_dummy_activity(plan=plan, usages=["input"], generations=["other"])

    activity_gateway = ActivityGateway()

    activity_gateway.add(upstream)
    activity_gateway.add(activity)
    activity_gateway.add(downstream)
    activity_gateway.add(other)

    # NOTE: Remove fails if activity has downstream and force is not set
    with pytest.raises(errors.ActivityDownstreamNotEmptyError):
        activity_gateway.remove(activity)

    assert len(activity_gateway.get_downstream_activities(activity)) > 0
    assert len(activity_gateway.get_upstream_activities(activity)) > 0
    assert "origin" in activity_gateway.get_all_usage_paths()
    assert [activity] == activity_gateway.get_activities_by_usage("origin")
    assert [activity, other] == activity_gateway.get_activities_by_usage("input")
    assert "intermediate" in activity_gateway.get_all_generation_paths()
    assert [activity] == activity_gateway.get_activities_by_generation("intermediate")
    assert [activity, other] == activity_gateway.get_activities_by_generation("other")
    assert {activity, downstream, other} == activity_gateway.get_downstream_activities(upstream)
    assert {activity, upstream} == activity_gateway.get_upstream_activities(downstream)

    activity_gateway.remove(activity, keep_reference=True, force=True)
    activity.delete()

    # Deleted activity is in the list of activities if we keep its reference and request it
    assert activity in activity_gateway.get_all_activities(include_deleted=True)

    # Deleted activity won't be listed normally
    assert activity not in activity_gateway.get_all_activities()

    # Activity doesn't have upstream or downstream anymore
    assert set() == activity_gateway.get_downstream_activities(activity)
    assert set() == activity_gateway.get_upstream_activities(activity)

    # Activity's usages are removed if it's the only user
    assert "origin" not in activity_gateway.get_all_usage_paths()
    assert [] == activity_gateway.get_activities_by_usage("origin")

    # Activity is removed from the list of users if it's not the only user
    assert "input" in activity_gateway.get_all_usage_paths()
    assert [other] == activity_gateway.get_activities_by_usage("input")

    # Activity's generations are removed if it's the only generator
    assert "intermediate" not in activity_gateway.get_all_generation_paths()
    assert [] == activity_gateway.get_activities_by_generation("intermediate")

    # Activity is removed from the list of generator if it's not the only generator
    assert "other" in activity_gateway.get_all_generation_paths()
    assert [other] == activity_gateway.get_activities_by_generation("other")

    # Relation chain of activities is cut when an activity is removed
    assert {other} == activity_gateway.get_downstream_activities(upstream)
    assert set() == activity_gateway.get_upstream_activities(downstream)

    activity_gateway.remove(downstream, keep_reference=False)

    # Activity won't be in the list of activities if we don't keep its reference
    assert downstream not in activity_gateway.get_all_activities()
