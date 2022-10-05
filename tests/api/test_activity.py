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
"""Tests for Activity API."""

import os

from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.ui.api import Activity, Input, Output, Parameter
from tests.utils import create_dummy_activity


def test_list_activities(project_with_runs):
    """Test listing activities."""
    activities = Activity.list()

    assert 2 == len(activities)
    assert isinstance(activities[0], Activity)
    assert isinstance(activities[1], Activity)


def test_list_datasets_outside_a_renku_project(directory_tree):
    """Test listing activities in a non-renku directory."""
    os.chdir(directory_tree)

    assert [] == Activity.list()


def test_get_activity_attributes(project_with_runs):
    """Test getting attributes of an activity."""
    activity = next(p for p in Activity.list() if p.plan.name == "plan-1")

    assert "/activities/1" == activity.id
    assert {"input"} == {p.path for p in activity.used_inputs}
    assert {"intermediate"} == {p.path for p in activity.generated_outputs}
    assert "command-1 -n 42 input > intermediate" == activity.executed_command
    assert "2022-05-20T00:42:00" == activity.started_at.isoformat()
    assert "2022-05-20T00:42:01" == activity.ended_at.isoformat()
    assert "Renku Bot <renku@datascience.ch>" == activity.user


def test_get_activity_values(project_with_runs):
    """Test getting values used in an activity."""
    activity = next(p for p in Activity.list() if p.plan.name == "plan-1")

    assert {"input-1", "output-1", "parameter-1"} == {p.field.name for p in activity.values}

    value = next(p for p in activity.values if p.field.name == "input-1")

    assert isinstance(value.field, Input)
    assert "input" == value.value

    value = next(p for p in activity.values if p.field.name == "output-1")

    assert isinstance(value.field, Output)
    assert "intermediate" == value.value

    value = next(p for p in activity.values if p.field.name == "parameter-1")

    assert isinstance(value.field, Parameter)
    assert "42" == value.value


def test_get_activity_downstreams(project_with_runs):
    """Test getting downstream of an activity."""
    activity = next(p for p in Activity.list() if p.plan.name == "plan-1")

    downstreams = activity.following_activities

    assert 1 == len(downstreams)
    assert "plan-2" == downstreams[0].plan.name

    activity = next(p for p in Activity.list() if p.plan.name == "plan-2")

    assert [] == activity.following_activities


def test_get_activity_upstreams(project_with_runs):
    """Test getting upstream of an activity."""
    activity = next(p for p in Activity.list() if p.plan.name == "plan-1")

    assert [] == activity.preceding_activities

    activity = next(p for p in Activity.list() if p.plan.name == "plan-2")

    upstreams = activity.preceding_activities

    assert 1 == len(upstreams)
    assert "plan-1" == upstreams[0].plan.name


def test_filter_activities(project_with_runs, with_injection):
    """Test Activity.filter method."""
    with with_injection(project_with_runs):
        activity_gateway = ActivityGateway()
        activity = next(a for a in activity_gateway.get_all_activities() if a.association.plan.name == "plan-2")
        plan = activity.association.plan

        activities = [
            create_dummy_activity(plan, index=3, generations=["some/"]),
            create_dummy_activity(plan, index=4, usages=["an/input"], generations=["some/output/files"]),
            create_dummy_activity(plan, index=5, parameters={"int-parameter": 420, "str-parameter": "new values"}),
            create_dummy_activity(plan, index=6, parameters={"int-parameter": 42000}),
        ]
        for activity in activities:
            activity_gateway.add(activity)

    assert {"/activities/1"} == {a.id for a in Activity.filter(inputs="input")}
    assert {"/activities/1"} == {a.id for a in Activity.filter(inputs=["input"])}
    assert {"/activities/1"} == {a.id for a in Activity.filter(inputs=lambda name: name == "input")}

    assert {"/activities/3", "/activities/4"} == {a.id for a in Activity.filter(outputs="some")}

    assert {"/activities/4"} == {a.id for a in Activity.filter(inputs="an/input", outputs=["so*"])}

    assert {"/activities/5"} == {a.id for a in Activity.filter(values=["new values"])}
    assert {"/activities/2", "/activities/5", "/activities/6"} == {
        a.id for a in Activity.filter(parameters=lambda name: name.endswith("parameter"))
    }
    assert {"/activities/5"} == {
        a.id for a in Activity.filter(parameters=["int-parameter", "str-parameter"], values=["new values"])
    }
    assert {"/activities/5", "/activities/6"} == {
        a.id for a in Activity.filter(values=lambda value: value >= 420 if isinstance(value, int) else False)
    }
