# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

import copy
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Optional, Set, Tuple, Union, cast
from uuid import uuid4

from renku.core import errors
from renku.core.util.datetime8601 import local_now
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    CommandParameterBase,
    ParameterLink,
    ParameterMapping,
)
from renku.domain_model.workflow.plan import MAX_GENERATED_NAME_LENGTH, AbstractPlan, Plan


class CompositePlan(AbstractPlan):
    """A plan containing child plans."""

    annotations: List[Annotation] = list()

    def __init__(
        self,
        *,
        derived_from: Optional[str] = None,
        description: Optional[str] = None,
        id: str,
        date_created: Optional[datetime] = None,
        date_modified: Optional[datetime] = None,
        date_removed: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
        links: Optional[List[ParameterLink]] = None,
        mappings: Optional[List[ParameterMapping]] = None,
        name: str,
        plans: List[Union["CompositePlan", Plan]],
        project_id: Optional[str] = None,
        annotations: Optional[List[Annotation]] = None,
        creators: Optional[List[Person]] = None,
    ):
        super().__init__(
            derived_from=derived_from,
            description=description,
            id=id,
            date_created=date_created,
            date_modified=date_modified,
            date_removed=date_removed,
            keywords=keywords,
            name=name,
            project_id=project_id,
            creators=creators,
        )
        self.annotations: List[Annotation] = annotations or []

        self.plans: List[Union["CompositePlan", Plan]] = plans
        self.mappings: List[ParameterMapping] = mappings or []
        self.links: List[ParameterLink] = links or []

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

    def is_equal_to(self, other: "CompositePlan") -> bool:
        """Return true if plan hasn't changed from the other plan."""

        def are_equal_with_order(values, other_values):
            return len(values) == len(other_values) and all(s.is_equal_to(o) for s, o in zip(values, other_values))

        def are_equal(values, other_values):
            return len(values) == len(other_values) and set(values) == set(other_values)

        # TODO: Include ``annotations``, ``mappings``, and ``links`` if they are added to the workflow definition file
        return (
            self.name == other.name
            and self.description == other.description
            and self.project_id == other.project_id
            and are_equal(self.keywords, other.keywords)
            and are_equal(self.creators, other.creators)
            and are_equal_with_order(self.plans, other.plans)
        )

    def set_mappings_from_strings(self, mapping_strings: List[str]) -> None:
        """Set mappings by parsing mapping strings."""
        for mapping_string in mapping_strings:
            name, targets = mapping_string.split("=", maxsplit=1)

            if not targets:
                # remove mapping that has no target
                for mapping in self.mappings:
                    if mapping.name == name:
                        self.mappings.remove(mapping)
                        break
                else:
                    raise errors.MappingNotFoundError(mapping=name, workflow=self.name)
                continue

            target_names = targets.split(",")

            target_params = []

            for target in target_names:
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
                    default_value=None,
                )
            )

    def add_mapping(self, mapping: ParameterMapping) -> None:
        """Add a mapping to this run."""
        existing: Optional[ParameterMapping] = next((m for m in self.mappings if mapping.name == m.name), None)

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

            source_param, _ = self.resolve_mapping_path(source)

            if isinstance(source_param, ParameterMapping):
                sources = list(source_param.leaf_parameters)
            else:
                sources = [source_param]

            sinks_names = sinks.split(",")

            sink_params: List[CommandParameterBase] = []

            for sink in sinks_names:
                resolved_sink, _ = self.resolve_mapping_path(sink)

                if isinstance(resolved_sink, ParameterMapping):
                    # NOTE: can't link between mappings, only the parameters they map to
                    sink_params.extend(resolved_sink.leaf_parameters)
                else:
                    sink_params.append(resolved_sink)

            for link_source in sources:
                self.add_link(link_source, sink_params)

    def add_link(self, source: CommandParameterBase, sinks: List[CommandParameterBase]) -> None:
        """Validate and add a ParameterLink."""
        if not source:
            raise errors.ParameterLinkError("Parameter Link has no source.")

        if not sinks:
            raise errors.ParameterLinkError("Parameter Link has no sink(s).")

        if isinstance(source, (CommandInput, CommandParameter)):
            # TODO: Change this once parameters can be calculated and serve as output
            raise errors.ParameterLinkError(
                f"A parameter link can't originate in a command input or command parameter: {source.name}"
            )

        for sink in sinks:
            if isinstance(sink, CommandOutput):
                raise errors.ParameterLinkError(f"A parameter link can't end in a command output: '{sink.name}'.")

            source_wf = self.find_parameter_workflow(source)
            if source_wf is None:
                raise errors.ParameterLinkError(f"Couldn't find parameter {source}")

            sink_wf = self.find_parameter_workflow(sink)
            if sink_wf is None:
                raise errors.ParameterLinkError(f"Couldn't find parameter {sink}")

            if source_wf.find_parameter(sink) or sink_wf.find_parameter(source):
                raise errors.ParameterLinkError(
                    f"Parameter links have to link between different workflows, source '{source.name}' and "
                    f"sink '{sink.name}' are on the same workflow."
                )

            existing = list(self.find_link_by_target(cast(Union[CommandInput, CommandParameter], sink)))

            if existing:
                _, existing_wfs = zip(*existing)
                existing_wfs = ", ".join(w.name for w in existing_wfs)

                raise errors.ParameterLinkError(
                    f"Parameter link to sink '{sink.name}' already exists in workflows: {existing_wfs}"
                )

        self.links.append(ParameterLink(source=source, sinks=sinks, id=ParameterLink.generate_id(self.id)))

    def find_parameter(self, parameter: CommandParameterBase):
        """Check if a parameter exists on this run or one of its children."""
        if parameter in self.mappings:
            return True

        for plan in self.plans:
            if plan.find_parameter(parameter):
                return True

        return False

    def get_parameter_path(self, parameter: CommandParameterBase):
        """Get the path to a parameter inside this plan."""
        if parameter in self.mappings:
            return [self]

        for plan in self.plans:
            path = plan.get_parameter_path(parameter)
            if path is not None:
                return [self] + path

        return None

    def get_parameter_by_id(self, parameter_id: str) -> Optional[CommandParameterBase]:
        """Get a parameter on this plan by id."""
        mapping = next((p for p in self.mappings if parameter_id == p.id), None)

        if mapping:
            return mapping

        for plan in self.plans:
            parameter = plan.get_parameter_by_id(parameter_id)

            if parameter:
                return parameter

        return None

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> Optional[AbstractPlan]:
        """Return the workflow a parameter belongs to."""
        if parameter in self.mappings:
            return self

        for plan in self.plans:
            found = plan.find_parameter_workflow(parameter)
            if found:
                return found

        return None

    def find_link_by_target(self, target: Union[CommandInput, CommandParameter]):
        """Find a link on this or a child workflow that has target as a sink."""
        for link in self.links:
            if target in link.sinks:
                yield link, self

        for plan in self.plans:
            if isinstance(plan, CompositePlan):
                yield plan.find_link_by_target(target)

    def _map_all(self, selector: Callable[[Plan], List[CommandParameterBase]]) -> None:
        """Automatically map all base parameters matched by selection_lambda on this run."""

        for step in self.plans:
            if not isinstance(step, Plan):
                continue

            params = selector(step)

            existing = self._find_existing_mapping(params)
            non_mapped: Set[CommandParameterBase] = set(params) - existing.keys()

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
        self._map_all(lambda x: cast(List[CommandParameterBase], x.inputs))

    def map_all_outputs(self) -> None:
        """Map all unmapped outputs from child steps to the parent."""
        self._map_all(lambda x: cast(List[CommandParameterBase], x.outputs))

    def map_all_parameters(self) -> None:
        """Map all unmapped parameters from child steps to the parent."""
        self._map_all(lambda x: cast(List[CommandParameterBase], x.parameters))

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

    def resolve_mapping_path(self, mapping_path: str) -> Tuple[CommandParameterBase, Union["CompositePlan", Plan]]:
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
            if isinstance(workflow, CompositePlan):
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

    def _get_default_name(self) -> str:
        return uuid4().hex[:MAX_GENERATED_NAME_LENGTH]

    def derive(self, creator: Optional[Person] = None) -> "CompositePlan":
        """Create a new ``CompositePlan`` that is derived from self."""
        derived = copy.copy(self)
        derived.derived_from = self.id
        derived.date_modified = local_now()
        derived.plans = self.plans.copy()
        derived.mappings = self.mappings.copy()
        derived.links = self.links.copy()
        derived.assign_new_id()

        if creator and hasattr(creator, "email") and not any(c for c in self.creators if c.email == creator.email):
            self.creators.append(creator)

        return derived

    def is_derivation(self) -> bool:
        """Return if an ``CompositePlan`` has correct derived_from."""
        return self.derived_from is not None and self.id != self.derived_from
