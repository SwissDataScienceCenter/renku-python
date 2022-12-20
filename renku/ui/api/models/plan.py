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
r"""Renku API Plan.

``Plan`` and ``CompositePlan`` classes represent Renku workflow plans executed
in a Project. Each of these classes has a static ``list`` method that returns a
list of all active plans/composite-plans in a project:

.. code-block:: python

    from renku.api import Plan

    plans = Plan.list()

    composite_plans = CompositePlan.list()

"""

from datetime import datetime
from typing import List, Optional, Type, Union, cast

from renku.core import errors
from renku.domain_model.workflow import composite_plan as core_composite_plan
from renku.domain_model.workflow import plan as core_plan
from renku.ui.api.models.parameter import Input, Link, Mapping, Output, Parameter
from renku.ui.api.util import get_plan_gateway


class Plan:
    """API Plan."""

    def __init__(
        self,
        *,
        command: str,
        date_created: Optional[datetime] = None,
        deleted: bool = False,
        description: Optional[str] = None,
        id: str,
        inputs: List[Input] = None,
        keywords: Optional[List[str]] = None,
        name: Optional[str] = None,
        outputs: List[Output] = None,
        parameters: List[Parameter] = None,
        success_codes: Optional[List[int]] = None,
    ):
        self.command: str = command
        self.date_created: Optional[datetime] = date_created
        self.deleted: bool = deleted
        self.description: Optional[str] = description
        self.id: str = id
        self.inputs: List[Input] = inputs or []
        self.keywords: List[str] = keywords or []
        self.name: Optional[str] = name
        self.outputs: List[Output] = outputs or []
        self.parameters: List[Parameter] = parameters or []
        self.success_codes: List[int] = success_codes or []

    @classmethod
    def from_plan(cls, plan: core_plan.Plan) -> "Plan":
        """Create an instance from a core Plan model.

        Args:
            plan(core_plan.Plan): The core plan.

        Returns:
            Plan: An API Plan model.
        """
        return cls(
            command=plan.command,
            date_created=plan.date_created,
            deleted=plan.date_removed is not None,
            description=plan.description,
            id=plan.id,
            inputs=[Input.from_parameter(i) for i in plan.inputs],
            keywords=plan.keywords,
            name=plan.name,
            outputs=[Output.from_parameter(o) for o in plan.outputs],
            parameters=[Parameter.from_parameter(p) for p in plan.parameters],
            success_codes=plan.success_codes,
        )

    @staticmethod
    def list(include_deleted: bool = False) -> List[Union["Plan", "CompositePlan"]]:
        """List all plans in a project.

        Args:
            include_deleted(bool): Whether to include deleted plans.

        Returns:
            A list of all plans in the supplied project.
        """
        return _convert_plans(_list_plans(include_deleted=include_deleted, type=core_plan.Plan))

    def __repr__(self):
        return f"<Plan '{self.name}'>"

    @property
    def activities(self) -> List:
        """Return a list of upstream activities."""
        from renku.ui.api.models.activity import get_activities

        return get_activities(plan_id=self.id)

    def get_latest_version(self) -> "Plan":
        """Return the latest version (derivative) of this plan."""
        return cast(Plan, _get_latest_version(plan=self))


class CompositePlan:
    """API CompositePlan."""

    def __init__(
        self,
        *,
        date_created: Optional[datetime] = None,
        deleted: bool = False,
        description: Optional[str] = None,
        id: str,
        keywords: Optional[List[str]] = None,
        links: List[Link] = None,
        mappings: List[Mapping] = None,
        name: Optional[str] = None,
        plans: List[Union["CompositePlan", "Plan"]] = None,
    ):
        self.date_created: Optional[datetime] = date_created
        self.deleted: bool = deleted
        self.description: Optional[str] = description
        self.id: str = id
        self.keywords: List[str] = keywords or []
        self.links: List[Link] = links or []
        self.mappings: List[Mapping] = mappings or []
        self.name: Optional[str] = name
        self.plans: List[Union["CompositePlan", "Plan"]] = plans or []

    @classmethod
    def from_composite_plan(cls, composite_plan: core_composite_plan.CompositePlan) -> "CompositePlan":
        """Create an instance from a core CompositePlan model.

        Args:
            composite_plan(core_composite_plan.CompositePlan): The core composite plan.

        Returns:
            CompositePlan: An API CompositePlan instance.
        """
        return cls(
            date_created=composite_plan.date_created,
            deleted=composite_plan.date_removed is not None,
            description=composite_plan.description,
            id=composite_plan.id,
            keywords=composite_plan.keywords,
            links=[Link.from_link(link) for link in composite_plan.links],
            mappings=[Mapping.from_parameter(m) for m in composite_plan.mappings],
            name=composite_plan.name,
            plans=_convert_plans(composite_plan.plans),
        )

    @staticmethod
    def list(include_deleted: bool = False) -> List[Union["Plan", "CompositePlan"]]:
        """List all plans in a project.

        Args:
            include_deleted(bool): Whether to include deleted plans.

        Returns:
            A list of all plans in the supplied project.
        """
        return _convert_plans(_list_plans(include_deleted=include_deleted, type=core_composite_plan.CompositePlan))

    def __repr__(self):
        return f"<CompositePlan '{self.name}'>"

    @property
    def activities(self) -> List:
        """Return a list of upstream activities."""
        from renku.ui.api.models.activity import get_activities

        return get_activities(plan_id=self.id)

    def get_latest_version(self) -> "CompositePlan":
        """Return the latest version (derivative) of this plan."""
        return cast(CompositePlan, _get_latest_version(plan=self))


def _convert_plan(plan) -> Union[Plan, CompositePlan]:
    """Convert a core Plans/CompositePlans to API Plans/CompositePlans."""
    if isinstance(plan, core_plan.Plan):
        return Plan.from_plan(plan)
    elif isinstance(plan, core_composite_plan.CompositePlan):
        return CompositePlan.from_composite_plan(plan)

    raise errors.ParameterError(f"Invalid plan type: {type(plan)}")


def _convert_plans(plans: List[Union[core_plan.AbstractPlan]]) -> List[Union[Plan, CompositePlan]]:
    """Convert a list of core Plans/CompositePlans to API Plans/CompositePlans."""
    return [_convert_plan(p) for p in plans]


def _list_plans(
    include_deleted: bool, type: Optional[Type[Union[core_plan.Plan, core_composite_plan.CompositePlan]]]
) -> List[core_plan.AbstractPlan]:
    """List all plans in a project.

    Args:
        include_deleted(bool): Whether to include deleted plans.
        type(Optional[Type[Union[core_plan.Plan, core_composite_plan.CompositePlan]]]): The type of plan to list.
    Returns:
        A list of all plans in the supplied project.
    """
    plan_gateway = get_plan_gateway()
    if not plan_gateway:
        return []

    plans = plan_gateway.get_all_plans()
    derivatives_mapping = {p.derived_from: p for p in plans if p.derived_from is not None}

    def get_latest_plan(plan):
        while plan.id in derivatives_mapping:
            plan = derivatives_mapping[plan.id]
        return plan

    latest_plans = {get_latest_plan(p) for p in plans}

    if not include_deleted:
        latest_plans = {p for p in latest_plans if not p.deleted}

    return [p for p in latest_plans if type is None or isinstance(p, type)]


def _get_latest_version(plan: Union[Plan, CompositePlan]) -> Union[Plan, CompositePlan]:
    """Get the latest version (derivative) of a plan or the plan itself if it's already the latest version."""
    all_plans = _list_plans(include_deleted=False, type=None)
    derivatives_mapping = {p.derived_from: p for p in all_plans if p.derived_from is not None}

    derived_plan = None
    plan_id = plan.id
    while plan_id in derivatives_mapping:
        derived_plan = derivatives_mapping[plan_id]
        plan_id = derived_plan.id

    return _convert_plan(derived_plan) if derived_plan is not None else plan
