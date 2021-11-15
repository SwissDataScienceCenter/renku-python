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
"""CompositePlan view model."""

from typing import List, Optional

from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.parameter import ParameterLink, ParameterMapping
from renku.core.models.workflow.plan import AbstractPlan


class ParameterMappingViewModel:
    """View model for ``ParameterMapping``."""

    def __init__(self, name: str, default_value: str, maps_to: List[str], description: Optional[str] = None):
        self.name = name
        self.default_value = default_value
        self.maps_to = maps_to
        self.description = description

    @classmethod
    def from_mapping(cls, mapping: ParameterMapping):
        """Create view model from ``ParameterMapping``."""
        return cls(
            name=mapping.name,
            default_value=str(mapping.default_value),
            maps_to=[m.name for m in mapping.mapped_parameters],
            description=mapping.description,
        )


class ParameterLinkViewModel:
    """View model for ``ParameterLink``."""

    def __init__(self, source: str, sinks: List[str]):
        self.source = source
        self.sinks = sinks

    @classmethod
    def from_link(cls, link: ParameterLink, plan: AbstractPlan):
        """Create view model from ``ParameterLink``."""
        source_path = plan.get_parameter_path(link.source)
        source_path.append(link.source)
        source_path = ".".join(p.name for p in source_path[1:])

        sinks = []

        for sink in link.sinks:
            sink_path = plan.get_parameter_path(sink)
            sink_path.append(sink)
            sink_path = ".".join(p.name for p in sink_path[1:])
            sinks.append(sink_path)
        return cls(source=source_path, sinks=sinks)


class StepViewModel:
    """View model for ``Plan`` inside ``CompositePlan``."""

    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name


class CompositePlanViewModel:
    """A view model for a ``CompositePlan``."""

    def __init__(
        self,
        id: str,
        name: str,
        mappings: List[ParameterMappingViewModel],
        links: List[ParameterLinkViewModel],
        steps: List[StepViewModel],
        description: Optional[str] = None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.mappings = mappings
        self.links = links
        self.steps = steps
        self.full_command = ""

    @classmethod
    def from_composite_plan(cls, plan: CompositePlan):
        """Create view model from ``Plan``."""
        return cls(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            mappings=[ParameterMappingViewModel.from_mapping(mapping) for mapping in plan.mappings],
            links=[ParameterLinkViewModel.from_link(link, plan) for link in plan.links],
            steps=[StepViewModel(id=s.id, name=s.name) for s in plan.plans],
        )
