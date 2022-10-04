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
"""CompositePlan view model."""

import json
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional

from renku.command.view_model.agent import PersonViewModel
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    CommandParameterBase,
    ParameterLink,
    ParameterMapping,
)
from renku.domain_model.workflow.plan import AbstractPlan

ParameterReference = NamedTuple("ParameterReference", [("id", str), ("plan_id", str), ("type", str), ("name", str)])

parameter_type_mapping: Dict[type, str] = {
    CommandInput: "Input",
    CommandOutput: "Output",
    CommandParameter: "Parameter",
    ParameterMapping: "Mapping",
}


def _parameter_id_to_plan_id(parameter_id: str):
    """Extract plan id from a parameter id."""
    parts = parameter_id.split("/")
    parts = parts[:-2]
    id = "/".join(parts)
    return f"/{id}"


def _parameter_to_type_string(parameter: CommandParameterBase):
    """Get a type string for a parameter."""
    return parameter_type_mapping[type(parameter)]


class ParameterMappingViewModel:
    """View model for ``ParameterMapping``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        targets: List[CommandParameterBase],
        description: Optional[str] = None,
        plan_id: Optional[str] = None,
    ):
        self.name = name
        self.default_value = default_value
        self.maps_to = [t.name for t in targets]
        self.targets = [
            ParameterReference(t.id, _parameter_id_to_plan_id(t.id), _parameter_to_type_string(t), t.name)
            for t in targets
        ]
        self.description = description
        self.plan_id = plan_id
        self.type = "Mapping"

    @classmethod
    def from_mapping(cls, mapping: ParameterMapping, plan_id: Optional[str] = None):
        """Create view model from ``ParameterMapping``.

        Args:
            mapping(ParameterMapping): Mapping to create view model from.

        Returns:
            View model of mapping.
        """
        return cls(
            name=mapping.name,
            default_value=str(mapping.default_value),
            targets=mapping.mapped_parameters,
            description=mapping.description,
            plan_id=plan_id,
        )


class ParameterLinkViewModel:
    """View model for ``ParameterLink``."""

    def __init__(
        self,
        id: str,
        plan_id: str,
        source: str,
        sinks: List[str],
        source_entry: Optional[ParameterReference] = None,
        sink_entries: Optional[List[ParameterReference]] = None,
    ):
        self.id = id
        self.plan_id = plan_id
        self.source = source
        self.sinks = sinks
        self.source_entry = source_entry
        self.sink_entries = sink_entries
        self.type = "Link"

    @classmethod
    def from_link(cls, link: ParameterLink, plan: AbstractPlan):
        """Create view model from ``ParameterLink``.

        Args:
            link(ParameterLink): Link to get view model from.
            plan(AbstractPlan): Parent plan.

        Returns:
            View model for link.
        """
        source_path = plan.get_parameter_path(link.source)
        source_path.append(link.source)
        source_path = ".".join(p.name for p in source_path[1:])

        source_entry = ParameterReference(
            link.source.id,
            _parameter_id_to_plan_id(link.source.id),
            _parameter_to_type_string(link.source),
            link.source.name,
        )

        sinks = []
        sink_entries = []

        for sink in link.sinks:
            sink_path = plan.get_parameter_path(sink)
            sink_path.append(sink)
            sink_path = ".".join(p.name for p in sink_path[1:])
            sinks.append(sink_path)
            sink_entries.append(
                ParameterReference(
                    sink.id, _parameter_id_to_plan_id(sink.id), _parameter_to_type_string(sink), sink.name
                )
            )
        return cls(
            id=link.id,
            plan_id=plan.id,
            source=source_path,
            sinks=sinks,
            source_entry=source_entry,
            sink_entries=sink_entries,
        )


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
        created: datetime,
        mappings: List[ParameterMappingViewModel],
        links: List[ParameterLinkViewModel],
        steps: List[StepViewModel],
        keywords: List[str],
        description: Optional[str] = None,
        creators: Optional[List[PersonViewModel]] = None,
        annotations: Optional[str] = None,
        touches_existing_files: Optional[bool] = None,
        latest: Optional[str] = None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.created = created
        self.mappings = mappings
        self.links = links
        self.steps = steps
        self.creators = creators
        self.keywords = keywords
        self.annotations = annotations
        self.touches_existing_files = touches_existing_files
        self.latest = latest
        self.type = "CompositePlan"

    @classmethod
    def from_composite_plan(cls, plan: CompositePlan):
        """Create view model from ``Plan``.

        Args:
            plan(CompositePlan): Composite Plan to get view model from.

        Returns:
            View model of composite Plan.
        """
        return cls(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            created=plan.date_created,
            mappings=[ParameterMappingViewModel.from_mapping(mapping) for mapping in plan.mappings],
            links=[ParameterLinkViewModel.from_link(link, plan) for link in plan.links],
            steps=[StepViewModel(id=s.id, name=s.name) for s in plan.plans],
            creators=[PersonViewModel.from_person(p) for p in plan.creators] if plan.creators else None,
            keywords=plan.keywords,
            annotations=json.dumps([{"id": a.id, "body": a.body, "source": a.source} for a in plan.annotations])
            if plan.annotations
            else None,
            latest=getattr(plan, "latest", None),
            touches_existing_files=getattr(plan, "touches_existing_files", False),
        )
