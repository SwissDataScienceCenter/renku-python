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
"""Plan management."""

from datetime import datetime
from typing import Generator, Optional

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.domain_model.provenance.activity import Activity
from renku.domain_model.workflow.plan import AbstractPlan


@inject.autoparams()
def get_latest_plan(plan: Optional[AbstractPlan], plan_gateway: IPlanGateway) -> Optional[AbstractPlan]:
    """Return the latest version of a given plan in its derivative chain."""
    if plan is None:
        return None

    all_plans = plan_gateway.get_all_plans()

    child_plan: Optional[AbstractPlan] = plan
    while child_plan is not None:
        plan = child_plan
        child_plan = next((p for p in all_plans if p.derived_from is not None and p.derived_from == plan.id), None)

    return plan


@inject.autoparams()
def get_derivative_chain(
    plan: Optional[AbstractPlan], plan_gateway: IPlanGateway
) -> Generator[AbstractPlan, None, None]:
    """Return all plans in the derivative chain of a given plan including its parents/children and the plan itself."""
    if plan is None:
        return

    plan = get_latest_plan(plan)

    while plan is not None:
        yield plan
        plan = plan_gateway.get_by_id(plan.derived_from)


@inject.autoparams("plan_gateway")
def remove_plan(name_or_id: str, force: bool, plan_gateway: IPlanGateway, when: datetime = local_now()):
    """Remove the workflow by its name or id.

    Args:
        name_or_id (str): The name of the Plan to remove.
        force (bool): Whether to force removal or not.
        plan_gateway(IPlanGateway): The injected Plan gateway.
        when(datetime): Time of deletion (Default value = current local date/time).
    Raises:
        errors.ParameterError: If the Plan doesn't exist or was already deleted.
    """
    plan: Optional[AbstractPlan] = plan_gateway.get_by_name(name_or_id) or plan_gateway.get_by_id(name_or_id)

    if not plan:
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    latest_version = get_latest_plan(plan)

    if latest_version.deleted:
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' is already deleted.")

    if not force:
        prompt_text = f"You are about to remove the following workflow '{name_or_id}'.\n\nDo you wish to continue?"
        communication.confirm(prompt_text, abort=True, warning=True)

    derived_plan = plan.derive()
    derived_plan.delete(when=when)

    plan_gateway.add(derived_plan)


@inject.autoparams()
def get_initial_id(plan: Optional[AbstractPlan], plan_gateway: IPlanGateway) -> Optional[str]:
    """Return the id of the first plan in the derivative chain."""
    if plan is None:
        return None

    if not plan.derived_from:
        return plan.id

    parent = plan_gateway.get_by_id(plan.derived_from)
    if parent is None:
        raise errors.ParameterError(f"Cannot find parent plan with id '{plan.derived_from}'")

    return get_initial_id(parent)


@inject.autoparams()
def get_activities(plan: Optional[AbstractPlan], activity_gateway: IActivityGateway) -> Generator[Activity, None, None]:
    """Return all valid activities that use the plan or one of its parent/child derivatives."""
    if plan is None:
        return

    derivative_ids = [p.id for p in get_derivative_chain(plan=plan)]

    for activity in activity_gateway.get_all_activities():
        if activity.deleted:
            continue

        if activity.association.plan.id in derivative_ids:
            yield activity


def is_plan_removed(plan: AbstractPlan) -> bool:
    """Return true if the plan or any plan in its derivative chain is deleted."""
    for derived_plan in get_derivative_chain(plan):
        if derived_plan.deleted:
            return True

    return False
