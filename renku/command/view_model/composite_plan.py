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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Union

from renku.command.view_model.agent import PersonViewModel
from renku.core.errors import ParameterNotFoundError
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

if TYPE_CHECKING:
    from renku.command.view_model.plan import PlanViewModel


ParameterReference = NamedTuple("ParameterReference", [("id", str), ("plan_id", str), ("type", str), ("name", str)])

parameter_type_mapping: Dict[type, str] = {
    CommandInput: "Input",
    CommandOutput: "Output",
    CommandParameter: "Parameter",
    ParameterMapping: "Mapping",
}


def _parameter_id_to_plan_id(parameter: CommandParameterBase, parent_plan: AbstractPlan, latest: bool = False):
    """Extract plan id from a parameter id."""
    from renku.core.workflow.plan import get_latest_plan

    plan_path = parent_plan.get_parameter_path(parameter)

    if plan_path is None or len(plan_path) < 1:
        raise ParameterNotFoundError(parameter.name, parent_plan.name)

    containing_plan = plan_path[-1]

    if latest:
        containing_plan = get_latest_plan(containing_plan)
    return containing_plan.id


def _parameter_to_type_string(parameter: CommandParameterBase):
    """Get a type string for a parameter."""
    return parameter_type_mapping[type(parameter)]


class ParameterMappingViewModel:
    """View model for ``ParameterMapping``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        maps_to: List[str],
        targets: List[ParameterReference],
        description: Optional[str] = None,
        plan_id: Optional[str] = None,
    ):
        self.name = name
        self.default_value = default_value
        self.maps_to = maps_to
        self.targets = targets
        self.description = description
        self.plan_id = plan_id
        self.type = "Mapping"

    @classmethod
    def from_mapping(cls, mapping: ParameterMapping, plan: AbstractPlan, latest: bool = False):
        """Create view model from ``ParameterMapping``.

        Args:
            mapping(ParameterMapping): Mapping to create view model from.
            plan(AbstractPlan): Parent plan.
            latest(bool): Whether to get latest plan data.

        Returns:
            View model of mapping.
        """
        targets = [
            ParameterReference(
                t.id, _parameter_id_to_plan_id(t, plan, latest=latest), _parameter_to_type_string(t), t.name
            )
            for t in mapping.mapped_parameters
        ]
        return cls(
            name=mapping.name,
            default_value=str(mapping.default_value),
            maps_to=[t.name for t in mapping.mapped_parameters],
            targets=targets,
            description=mapping.description,
            plan_id=plan.id,
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
    def from_link(cls, link: ParameterLink, plan: AbstractPlan, latest: bool = False):
        """Create view model from ``ParameterLink``.

        Args:
            link(ParameterLink): Link to get view model from.
            plan(AbstractPlan): Parent plan.
            latest(bool): Whether to get latest plan data.

        Returns:
            View model for link.
        """
        source_path = plan.get_parameter_path(link.source)
        source_path.append(link.source)
        source_path = ".".join(p.name for p in source_path[1:])

        source_entry = ParameterReference(
            link.source.id,
            _parameter_id_to_plan_id(link.source, plan, latest=latest),
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
                    sink.id,
                    _parameter_id_to_plan_id(sink, plan, latest=latest),
                    _parameter_to_type_string(sink),
                    sink.name,
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
        steps: List[Union["CompositePlanViewModel", "PlanViewModel"]],
        keywords: List[str],
        description: Optional[str] = None,
        creators: Optional[List[PersonViewModel]] = None,
        annotations: Optional[str] = None,
        touches_existing_files: Optional[bool] = None,
        latest: Optional[str] = None,
        duration: Optional[timedelta] = None,
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
        self.full_command = ""

        if duration is not None:
            self.duration = duration.seconds

    @classmethod
    def from_composite_plan(cls, plan: CompositePlan, latest: bool = False):
        """Create view model from ``Plan``.

        Args:
            plan(CompositePlan): Composite Plan to get view model from.
            latest(bool): Whether to get latest plan data.

        Returns:
            View model of composite Plan.
        """
        from renku.command.view_model.plan import plan_view

        return cls(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            created=plan.date_created,
            mappings=[ParameterMappingViewModel.from_mapping(mapping, plan, latest) for mapping in plan.mappings],
            links=[ParameterLinkViewModel.from_link(link, plan, latest) for link in plan.links],
            steps=[plan_view(p) for p in getattr(plan, "newest_plans", plan.plans)],
            creators=[PersonViewModel.from_person(p) for p in plan.creators] if plan.creators else None,
            keywords=plan.keywords,
            annotations=json.dumps([{"id": a.id, "body": a.body, "source": a.source} for a in plan.annotations])
            if plan.annotations
            else None,
            latest=getattr(plan, "latest", None),
            touches_existing_files=getattr(plan, "touches_existing_files", False),
            duration=getattr(plan, "duration", None),
        )
