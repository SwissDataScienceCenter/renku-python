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
in a Project. You can get a list of all active plans in a project by calling
the ``list`` static method of each of these classes:

.. code-block:: python

    from renku.ui.api import Plan

    datasets = Plan.list()

"""
from datetime import datetime
from typing import List, Optional, Union

from renku.command.command_builder.database_dispatcher import DatabaseDispatcher
from renku.core import errors
from renku.domain_model.workflow import composite_plan as core_composite_plan
from renku.domain_model.workflow import plan as core_plan
from renku.infrastructure.gateway.plan_gateway import PlanGateway
from renku.ui.api.models.project import ensure_project_context
from renku.ui.api.models.run import Input, Link, Mapping, Output, Parameter


class Plan:
    """API Plan."""

    def __init__(
        self,
        command: str,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        inputs: List[Input] = None,
        invalidated_at: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
        name: Optional[str] = None,
        outputs: List[Output] = None,
        parameters: List[Parameter] = None,
        success_codes: Optional[List[int]] = None,
    ):
        self.command: str = command
        self.date_created: Optional[datetime] = date_created
        self.description: Optional[str] = description
        self.inputs: List[Input] = inputs or []
        self.invalidated_at: Optional[datetime] = invalidated_at
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
            description=plan.description,
            inputs=[Input.from_parameter(i) for i in plan.inputs],
            invalidated_at=plan.invalidated_at,
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
        return _list_plans(include_deleted=include_deleted)

    @property
    def deleted(self) -> bool:
        """True if plan is deleted."""
        return self.invalidated_at is not None


class CompositePlan:
    """API CompositePlan."""

    def __init__(
        self,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        invalidated_at: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
        links: List[Link] = None,
        mappings: List[Mapping] = None,
        name: Optional[str] = None,
        plans: List[Union["CompositePlan", "Plan"]] = None,
    ):
        self.date_created: Optional[datetime] = date_created
        self.description: Optional[str] = description
        self.invalidated_at: Optional[datetime] = invalidated_at
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
            description=composite_plan.description,
            invalidated_at=composite_plan.invalidated_at,
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
        return _list_plans(include_deleted=include_deleted)

    @property
    def deleted(self) -> bool:
        """True if plan is deleted."""
        return self.invalidated_at is not None


def _convert_plans(plans: List[Union[core_plan.AbstractPlan]]) -> List[Union[Plan, CompositePlan]]:
    """Convert a list of core Plans/CompositePlans to API Plans/CompositePlans."""

    def convert_plan(plan):
        if isinstance(plan, core_plan.Plan):
            return Plan.from_plan(plan)
        elif isinstance(plan, core_composite_plan.CompositePlan):
            return CompositePlan.from_composite_plan(plan)

        raise errors.ParameterError(f"Invalid plan type: {type(plan)}")

    return [convert_plan(p) for p in plans]


@ensure_project_context
def _list_plans(include_deleted: bool, project) -> List[Union[Plan, CompositePlan]]:
    """List all plans in a project.

    Args:
        include_deleted(bool): Whether to include deleted plans.
        project: The current project

    Returns:
        A list of all plans in the supplied project.
    """
    client = project.client
    if not client:
        return []

    database_dispatcher = DatabaseDispatcher()
    database_dispatcher.push_database_to_stack(client.database_path)
    plan_gateway = PlanGateway()
    plan_gateway.database_dispatcher = database_dispatcher

    plans = plan_gateway.get_all_plans()

    if not include_deleted:
        plans = [p for p in plans if p.invalidated_at is None]

    return _convert_plans(plans)
