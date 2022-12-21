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
"""Renku plan gateway interface."""

from abc import ABC
from typing import Dict, List, Optional

from renku.domain_model.workflow.plan import AbstractPlan


class IPlanGateway(ABC):
    """Interface for the PlanGateway."""

    def get_by_id(self, id: Optional[str]) -> Optional[AbstractPlan]:
        """Get a plan by id."""
        raise NotImplementedError

    def get_by_name(self, name: str) -> Optional[AbstractPlan]:
        """Get a plan by name."""
        raise NotImplementedError

    def get_by_name_or_id(self, name_or_id: str) -> AbstractPlan:
        """Get a plan by name or id."""
        raise NotImplementedError()

    def list_by_name(self, starts_with: str, ends_with: str = None) -> List[str]:
        """Search plans by name."""
        raise NotImplementedError

    def get_newest_plans_by_names(self, include_deleted: bool = False) -> Dict[str, AbstractPlan]:
        """Return a mapping of all plan names to their newest plans."""
        raise NotImplementedError

    def get_all_plans(self) -> List[AbstractPlan]:
        """Get all plans in project."""
        raise NotImplementedError

    def add(self, plan: AbstractPlan) -> None:
        """Add a plan to the database."""
        raise NotImplementedError
