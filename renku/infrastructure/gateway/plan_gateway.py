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
"""Renku plan database gateway implementation."""

from typing import Dict, List, Optional, cast

from renku.core import errors
from renku.core.interface.plan_gateway import IPlanGateway
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.plan import AbstractPlan


class PlanGateway(IPlanGateway):
    """Gateway for plan database operations."""

    def get_by_id(self, id: Optional[str]) -> Optional[AbstractPlan]:
        """Get a plan by id."""
        return project_context.database["plans"].get(id)

    def get_by_name(self, name: str) -> Optional[AbstractPlan]:
        """Get a plan by name."""
        return project_context.database["plans-by-name"].get(name)

    def get_by_name_or_id(self, name_or_id: str) -> AbstractPlan:
        """Get a plan by name or id."""
        workflow = self.get_by_id(name_or_id) or self.get_by_name(name_or_id)

        if not workflow:
            raise errors.WorkflowNotFoundError(name_or_id)
        return workflow

    def list_by_name(self, starts_with: str, ends_with: str = None) -> List[str]:
        """Search plans by name."""
        return [
            name
            for name in project_context.database["plans-by-name"].keys(min=starts_with, max=ends_with)
            if not cast(AbstractPlan, self.get_by_name(name)).deleted
        ]

    def get_newest_plans_by_names(self, include_deleted: bool = False) -> Dict[str, AbstractPlan]:
        """Return a mapping of all plan names to their newest plans."""
        database = project_context.database
        if include_deleted:
            return dict(database["plans-by-name"])
        return {k: v for k, v in database["plans-by-name"].items() if not v.deleted}

    def get_all_plans(self) -> List[AbstractPlan]:
        """Get all plans in project."""
        return list(project_context.database["plans"].values())

    def add(self, plan: AbstractPlan) -> None:
        """Add a plan to the database."""
        database = project_context.database

        if database["plans"].get(plan.id) is not None:
            return

        database["plans"].add(plan)

        if plan.derived_from is not None:
            derived_from = self.get_by_id(plan.derived_from)

            if derived_from is not None:
                database["plans-by-name"].pop(derived_from.name, None)
        database["plans-by-name"].add(plan)
