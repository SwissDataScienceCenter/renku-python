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
"""Renku activity gateway interface."""

from abc import ABC
from typing import Dict, List, Set

from renku.core.models.provenance.activity import Activity, Usage
from renku.core.models.workflow.plan import AbstractPlan


class IActivityGateway(ABC):
    """Interface for the ActivityGateway."""

    def get_latest_activity_per_plan(self):
        """Get latest activity for each plan."""
        raise NotImplementedError

    def get_plans_and_usages_for_latest_activities(self) -> Dict[AbstractPlan, List[Usage]]:
        """Get all usages associated with a plan by its latest activity."""
        raise NotImplementedError

    def get_all_usage_paths(self) -> List[str]:
        """Return all usage paths."""
        raise NotImplementedError

    def get_all_generation_paths(self) -> List[str]:
        """Return all generation paths."""
        raise NotImplementedError

    def get_downstream_activities(self, activity: Activity, max_depth=None) -> Set[Activity]:
        """Get downstream activities that depend on this activity."""
        raise NotImplementedError

    def get_all_activities(self) -> List[Activity]:
        """Get all activities in the project."""
        raise NotImplementedError

    def add(self, activity: Activity) -> None:
        """Add an ``Activity`` to storage."""
        raise NotImplementedError
