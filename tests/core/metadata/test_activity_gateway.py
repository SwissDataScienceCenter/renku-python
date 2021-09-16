# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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

from datetime import datetime, timedelta

from renku.core.metadata.gateway.activity_gateway import ActivityGateway
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.workflow.plan import Plan


def test_activity_gateway_get_latest_activity(dummy_database_injection_manager):
    """Test getting latest activity for a plan."""

    plan = Plan(id=Plan.generate_id(), name="plan")
    plan2 = Plan(id=Plan.generate_id(), name="plan2")

    activity1_id = Activity.generate_id()
    activity1 = Activity(
        id=activity1_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(activity1_id), plan=plan),
    )

    activity2_id = Activity.generate_id()
    activity2 = Activity(
        id=activity2_id,
        ended_at_time=datetime.utcnow(),
        association=Association(id=Association.generate_id(activity2_id), plan=plan),
    )

    activity3_id = Activity.generate_id()
    activity3 = Activity(
        id=activity3_id,
        ended_at_time=datetime.utcnow(),
        association=Association(id=Association.generate_id(activity3_id), plan=plan2),
    )

    with dummy_database_injection_manager(None):
        activity_gateway = ActivityGateway()

        activity_gateway.add(activity1)

        latest_activities = activity_gateway.get_latest_activity_per_plan()
        assert len(latest_activities) == 1
        assert {activity1_id} == {a.id for a in latest_activities.values()}
        assert {plan.id} == {p.id for p in latest_activities.keys()}

        activity_gateway.add(activity3)

        latest_activities = activity_gateway.get_latest_activity_per_plan()
        assert len(latest_activities) == 2
        assert {activity1_id, activity3_id} == {a.id for a in latest_activities.values()}
        assert {plan.id, plan2.id} == {p.id for p in latest_activities.keys()}

        activity_gateway.add(activity2)

        latest_activities = activity_gateway.get_latest_activity_per_plan()
        assert len(latest_activities) == 2
        assert {activity2_id, activity3_id} == {a.id for a in latest_activities.values()}
        assert {plan.id, plan2.id} == {p.id for p in latest_activities.keys()}


def test_activity_gateway_get_latest_plan_usages(dummy_database_injection_manager):
    """Test getting latest activity for a plan."""

    plan = Plan(id=Plan.generate_id(), name="plan")
    plan2 = Plan(id=Plan.generate_id(), name="plan2")

    activity1_id = Activity.generate_id()
    activity1 = Activity(
        id=activity1_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(activity1_id), plan=plan),
        usages=[
            Usage(
                id=Usage.generate_id(activity1_id),
                entity=Entity(id=Entity.generate_id("abcdefg", "in1"), checksum="abcdefg", path="in1"),
            )
        ],
    )

    activity2_id = Activity.generate_id()
    activity2 = Activity(
        id=activity2_id,
        ended_at_time=datetime.utcnow(),
        association=Association(id=Association.generate_id(activity2_id), plan=plan),
        usages=[
            Usage(
                id=Usage.generate_id(activity2_id),
                entity=Entity(id=Entity.generate_id("abcdefg", "in1"), checksum="abcdefg", path="in1"),
            )
        ],
    )

    activity3_id = Activity.generate_id()
    activity3 = Activity(
        id=activity3_id,
        ended_at_time=datetime.utcnow(),
        association=Association(id=Association.generate_id(activity3_id), plan=plan2),
        usages=[
            Usage(
                id=Usage.generate_id(activity3_id),
                entity=Entity(id=Entity.generate_id("abcdefg", "in2"), checksum="abcdefg", path="in2"),
            )
        ],
    )

    with dummy_database_injection_manager(None):
        activity_gateway = ActivityGateway()

        activity_gateway.add(activity1)
        activity_gateway.add(activity2)
        activity_gateway.add(activity3)

        latest_usages = activity_gateway.get_plans_and_usages_for_latest_activities()

        assert len(latest_usages) == 2
        assert latest_usages[plan] == activity2.usages
        assert latest_usages[plan2] == activity3.usages


def test_activity_gateway_downstream_activities_and_chains(dummy_database_injection_manager):
    """test getting downstream activities and activity chains work."""

    plan = Plan(id=Plan.generate_id(), name="plan")

    intermediate_id = Activity.generate_id()
    intermediate = Activity(
        id=intermediate_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(intermediate_id), plan=plan),
        generations=[
            Generation(
                id=Generation.generate_id(intermediate_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "intermediate_out"), checksum="abcdefg", path="intermediate_out"
                ),
            )
        ],
        usages=[
            Usage(
                id=Usage.generate_id(intermediate_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "intermediate_in"), checksum="abcdefg", path="intermediate_in"
                ),
            )
        ],
    )

    previous_id = Activity.generate_id()
    previous = Activity(
        id=previous_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(previous_id), plan=plan),
        generations=[
            Generation(
                id=Generation.generate_id(previous_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "intermediate_in"), checksum="abcdefg", path="intermediate_in"
                ),
            )
        ],
    )

    following_id = Activity.generate_id()
    following = Activity(
        id=following_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(following_id), plan=plan),
        usages=[
            Usage(
                id=Usage.generate_id(following_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "intermediate_out"), checksum="abcdefg", path="intermediate_out"
                ),
            )
        ],
    )

    unrelated_id = Activity.generate_id()
    unrelated = Activity(
        id=unrelated_id,
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(unrelated_id), plan=plan),
        generations=[
            Generation(
                id=Generation.generate_id(unrelated_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "unrelated_out"), checksum="abcdefg", path="unrelated_out"
                ),
            )
        ],
        usages=[
            Usage(
                id=Usage.generate_id(unrelated_id),
                entity=Entity(
                    id=Entity.generate_id("abcdefg", "unrelated_in"), checksum="abcdefg", path="unrelated_in"
                ),
            )
        ],
    )

    with dummy_database_injection_manager(None):
        activity_gateway = ActivityGateway()

        activity_gateway.add(intermediate)
        activity_gateway.add(following)
        activity_gateway.add(previous)
        activity_gateway.add(unrelated)

        downstream = activity_gateway.get_downstream_activities(following)

        assert not downstream

        downstream = activity_gateway.get_downstream_activities(intermediate)
        assert {following_id} == {a.id for a in downstream}

        downstream = activity_gateway.get_downstream_activities(previous)
        assert {following_id, intermediate_id} == {a.id for a in downstream}

        downstream_chains = activity_gateway.get_downstream_activity_chains(following)

        assert not downstream_chains

        downstream_chains = activity_gateway.get_downstream_activity_chains(intermediate)
        assert [{following_id}] == [{a.id for a in chain} for chain in downstream_chains]

        downstream_chains = activity_gateway.get_downstream_activity_chains(previous)
        assert [{intermediate_id}, {following_id, intermediate_id}] == [
            {a.id for a in chain} for chain in downstream_chains
        ]
