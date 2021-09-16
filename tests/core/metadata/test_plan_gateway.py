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
"""Test plan database gateways."""

from datetime import datetime

from renku.core.metadata.gateway.plan_gateway import PlanGateway
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import Plan


def test_plan_gateway_add_get(dummy_database_injection_manager):
    """Test getting a plan by id."""

    plan = Plan(id=Plan.generate_id(), name="plan")
    composite_plan = CompositePlan(id=CompositePlan.generate_id(), name="composite-plan")

    with dummy_database_injection_manager(None):
        plan_gateway = PlanGateway()

        plan_gateway.add(plan)
        plan_gateway.add(composite_plan)

        assert plan == plan_gateway.get_by_id(plan.id)
        assert composite_plan == plan_gateway.get_by_id(composite_plan.id)

        assert plan == plan_gateway.get_by_name("plan")
        assert composite_plan == plan_gateway.get_by_name("composite-plan")

        assert not plan_gateway.get_by_name(plan.id)
        assert not plan_gateway.get_by_name(composite_plan.id)

        assert not plan_gateway.get_by_id("plan")
        assert not plan_gateway.get_by_id("composite-plan")


def test_plan_gateway_newest_plans(dummy_database_injection_manager, database):
    """Test getting newest plans."""
    plan = Plan(id=Plan.generate_id(), name="plan")
    plan2 = Plan(id=Plan.generate_id(), name="plan")
    invalidated_plan = Plan(id=Plan.generate_id(), name="invalidated_plan", invalidated_at=datetime.utcnow())
    invalidated_plan2 = Plan(id=Plan.generate_id(), name="invalidated_plan", invalidated_at=datetime.utcnow())

    with dummy_database_injection_manager(None):
        plan_gateway = PlanGateway()

        plan_gateway.add(plan)
        plan_gateway.add(plan2)
        plan_gateway.add(invalidated_plan)
        plan_gateway.add(invalidated_plan2)

        newest_plans_by_names = {p.id for p in plan_gateway.get_newest_plans_by_names().values()}

        assert {plan2.id} == newest_plans_by_names

        newest_plans_by_names_with_invalidated = {
            p.id for p in plan_gateway.get_newest_plans_by_names(with_invalidated=True).values()
        }

        assert {plan2.id, invalidated_plan2.id} == newest_plans_by_names_with_invalidated
