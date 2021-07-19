# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Represent a group of run templates."""

from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Union
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core import errors
from renku.core.metadata.database import Persistent
from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    CommandParameterBase,
    ParameterLink,
    ParameterLinkSchema,
    ParameterMapping,
    ParameterMappingSchema,
)
from renku.core.models.workflow.plan import Plan, PlanSchema


class GroupedRun(Persistent):
    """A plan containing child plans."""

    def __init__(
        self,
        *,
        description: str = None,
        id: str,
        invalidated_at: datetime = None,
        keywords: List[str] = None,
        name: str,
        plans: List[Plan] = None,
        mappings: List[ParameterMapping] = None,
        links: List[ParameterLink] = None,
    ):
        self.description: str = description
        self.id: str = id
        self.invalidated_at: datetime = invalidated_at
        self.keywords: List[str] = keywords or []
        self.name: str = name

        Plan.validate_name(name)

        self.plans: List[Plan] = plans
        self.mappings: List[ParameterMapping] = mappings or []
        self.links: List[ParameterLink] = links or []

    @staticmethod
    def generate_id(uuid: str = None) -> str:
        """Generate an identifier for Plan."""
        uuid = uuid or str(uuid4())
        return f"/plans/{uuid}"

    def _find_existing_mapping(
        self, targets: List[CommandParameterBase]
    ) -> Dict[CommandParameterBase, List[ParameterMapping]]:
        """Find if any mapping already exists for any of the targets."""
        existing = defaultdict(list)
        for target in targets:
            found = [m for m in self.mappings if target in m.mapped_parameters]

            if found:
                existing[target].extend(found)

        return dict(existing)

    def set_mappings_from_strings(self, mapping_strings: List[str]) -> None:
        """Set mappings by parsing mapping strings."""
        for mapping_string in mapping_strings:
            name, targets = mapping_string.split("=", maxsplit=1)
            targets = targets.split(",")

            target_params = []

            for target in targets:
                target_params.append(self.resolve_mapping_path(target)[0])

            existing = self._find_existing_mapping(target_params)

            if existing:
                raise errors.MappingExistsError(
                    [k.name + ": " + ", ".join(m.name for m in v) for k, v in existing.items()]
                )

            self.add_mapping(
                ParameterMapping(
                    name=name,
                    mapped_parameters=target_params,
                    id=ParameterMapping.generate_id(plan_id=self.id, postfix=name),
                )
            )

    def add_mapping(self, mapping: ParameterMapping) -> None:
        """Add a mapping to this run."""
        existing: ParameterMapping = next((m for m in self.mappings if mapping.name == m.name), None)

        if not existing:
            self.mappings.append(mapping)
            return

        new = set(mapping.mapped_parameters) - set(existing.mapped_parameters)

        if new:
            existing.mapped_parameters.extend(new)

    def set_links_from_strings(self, link_strings: List[str]) -> None:
        """Set links between parameters of child steps."""
        for link_string in link_strings:
            source, sinks = link_string.split("=", maxsplit=1)

            source, source_wf = self.resolve_mapping_path(source)

            if isinstance(source, (CommandInput, CommandParameter)):
                # TODO: Change this once parameters can be calculated and serve as output
                raise errors.ParameterLinkError(
                    f"A parameter link can't originate in a command input or command parameter: {source.name}"
                )

            if source in self.mappings:
                raise errors.ParameterLinkError(
                    f"Parameter links can only be between child steps, source '{source.name}' is on the parent."
                )

            sinks = sinks.split(",")

            sink_params = []

            for sink in sinks:
                resolved_sink, sink_wf = self.resolve_mapping_path(sink)

                sink_params.append(resolved_sink)

            self.add_link(source, sink_params)

    def add_link(self, source: CommandParameterBase, sinks: List[CommandParameterBase]) -> None:
        """Validate and add a ParameterLink."""
        if not source:
            raise errors.ParameterLinkError("Parameter Link has no source.")

        if not sinks:
            raise errors.ParameterLinkError("Parameter Link has no sink(s).")

        for sink in sinks:
            if isinstance(sink, CommandOutput):
                raise errors.ParameterLinkError(f"A parameter link can't end in a command output: '{sink.name}'.")

            if sink in self.mappings:
                raise errors.ParameterLinkError(
                    f"Parameter links can only be between child steps, sink '{sink.name}' is on " "the parent."
                )

            source_wf = self.find_parameter_workflow(source)
            sink_wf = self.find_parameter_workflow(sink)

            if source_wf.find_parameter(sink) or sink_wf.find_parameter(source):
                raise errors.ParameterLinkError(
                    f"Parameter links have to link between different workflows, source '{source.name}' and "
                    f"sink '{sink.name}' are on the same workflow."
                )

        self.links.append(ParameterLink(source=source, sinks=sinks, id=ParameterLink.generate_id(self.id)))

    def find_parameter(self, parameter: CommandParameterBase):
        """Check if a parameter exists on this run or one of its children."""
        if parameter in self.mappings:
            return True

        found = False

        for plan in self.plans:
            found = found or plan.find_parameter(parameter)

        return found

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> Union["GroupedRun", Plan]:
        """Return the workflow a parameter belongs to."""
        if parameter in self.mappings:
            return self

        for plan in self.plans:
            found = plan.find_parameter_workflow(parameter)
            if found:
                return found

        return None

    def _map_all(self, selector: Callable[[Plan], CommandParameterBase]) -> None:
        """Automatically map all base parameters matched by selection_lambda on this run."""

        for step in self.plans:
            if not isinstance(step, Plan):
                continue

            params = selector(step)

            existing = self._find_existing_mapping(params)
            non_mapped = set(params) - existing.keys()

            for param in non_mapped:
                name = f"{step.name}_{param.name}"
                self.add_mapping(
                    ParameterMapping(
                        name=name,
                        mapped_parameters=[param],
                        id=ParameterMapping.generate_id(
                            plan_id=self.id,
                            postfix=name,
                        ),
                        default_value=param.default_value,
                    )
                )

    def map_all_inputs(self) -> None:
        """Map all unmapped inputs from child steps to the parent."""
        self._map_all(lambda x: x.inputs)

    def map_all_outputs(self) -> None:
        """Map all unmapped outputs from child steps to the parent."""
        self._map_all(lambda x: x.outputs)

    def map_all_parameters(self) -> None:
        """Map all unmapped parameters from child steps to the parent."""
        self._map_all(lambda x: x.parameters)

    def set_mapping_defaults(self, default_strings: List[str]) -> None:
        """Set default value based on a default specification string."""
        for default_string in default_strings:
            target, value = default_string.split("=", maxsplit=1)

            mapping, _ = self.resolve_mapping_path(target)

            mapping.default_value = value

    def set_mapping_descriptions(self, mapping_descriptions: List[str]) -> None:
        """Set descriptions for mappings."""

        for mapping_description in mapping_descriptions:
            target, value = mapping_description.split("=", maxsplit=1)

            mapping = self.resolve_direct_reference(target)
            mapping.description = value.strip(' "')

    def resolve_mapping_path(self, mapping_path: str) -> CommandParameterBase:
        """Resolve a mapping path to its reference parameter."""

        parts = mapping_path.split(".", maxsplit=1)

        if len(parts) == 1:
            return self.resolve_direct_reference(parts[0]), self

        prefix, suffix = parts

        if prefix.startswith("@step"):
            # NOTE: relative reference
            try:
                workflow = self.plans[int(prefix[5:]) - 1]
            except (ValueError, IndexError):
                raise errors.ParameterNotFoundError(mapping_path, self.name)
            if isinstance(workflow, GroupedRun):
                return workflow.resolve_mapping_path(suffix)
            else:
                return workflow.resolve_direct_reference(suffix), workflow

        for workflow in self.plans:
            if workflow.name == prefix:
                return workflow.resolve_mapping_path(suffix)

        raise errors.ParameterNotFoundError(mapping_path, self.name)

    def resolve_direct_reference(self, reference: str) -> CommandParameterBase:
        """Resolve a direct parameter reference."""
        if reference.startswith("@mapping"):
            try:
                return self.mappings[int(reference[8:]) - 1]
            except (ValueError, IndexError):
                raise errors.ParameterNotFoundError(reference, self.name)

        for mapping in self.mappings:
            if mapping.name == reference:
                return mapping

        raise errors.ParameterNotFoundError(reference, self.name)


class GroupedRunSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork, renku.GroupedRun]
        model = GroupedRun
        unknown = EXCLUDE

    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    mappings = Nested(renku.hasMappings, [ParameterMappingSchema], many=True, missing=None)
    invalidated_at = fields.DateTime(prov.invalidatedAtTime, add_value_types=True)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    name = fields.String(schema.name, missing=None)
    plans = Nested(renku.hasSubprocess, [PlanSchema, "GroupedRunSchema"], many=True)
    links = Nested(renku.workflowLinks, [ParameterLinkSchema], many=True, missing=None)
