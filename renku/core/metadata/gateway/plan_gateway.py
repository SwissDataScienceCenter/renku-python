# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

from typing import Dict

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.metadata.database import Database
from renku.core.models.workflow.plan import AbstractPlan


class PlanGateway(IPlanGateway):
    """Gateway for plan database operations."""

    database = inject.attr(Database)

    def get_by_id(self, id: str) -> AbstractPlan:
        """Get a plan by id."""
        return self.database["plans"].get(id)

    def get_by_name(self, name: str) -> AbstractPlan:
        """Get a plan by name."""
        return self.database["plans-by-name"].get(name)

    def get_newest_plans_by_names(self, with_invalidated: bool = False) -> Dict[str, AbstractPlan]:
        """Return a list of all newest plans with their names."""
        if with_invalidated:
            return dict(self.database["plans-by-name"])
        return {k: v for k, v in self.database["plans-by-name"].items() if v.invalidated_at is None}

    def add(self, plan: AbstractPlan) -> None:
        """Add a plan to the database."""
        self.database["plans"].add(plan)
        self.database["plans-by-name"].add(plan)
