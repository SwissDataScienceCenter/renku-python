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
"""Resolution of Worklow execution values precedence."""

from typing import Any, Dict, Union

from renku.core import errors
from renku.core.models.workflow.grouped_run import GroupedRun
from renku.core.models.workflow.parameter import ParameterMapping
from renku.core.models.workflow.plan import Plan


def apply_run_values(workflow: Union[GroupedRun, Plan], values: Dict[str, Any] = None) -> None:
    """Applies values and default_values to a potentially nested workflow."""

    if isinstance(workflow, Plan):
        return apply_single_run_values(workflow, values)

    return apply_composite_run_values(workflow, values)


def apply_single_run_values(workflow: Plan, values: Dict[str, Any] = None) -> None:
    """Applies values and default_values to a workflow."""
    if values:
        for param in workflow.inputs + workflow.outputs + workflow.parameters:
            if param.name in values:
                param.actual_value = values[param.name]

    return workflow


def apply_composite_run_values(workflow: GroupedRun, values: Dict[str, Any] = None) -> None:
    """Applies values and default_values to a nested workflow."""

    if values:
        if "parameters" in values:
            # NOTE: Set mapping parameter values
            apply_parameters_values(workflow, values["parameters"], override=True)

        if "steps" in values:
            for name, step in values["steps"].items():
                child_workflow = next((w for w in workflow.plans if w.name == name), None)
                if not child_workflow:
                    raise errors.ChildWorkflowNotFound(name, workflow.name)

                apply_run_values(child_workflow, step)

    # apply defaults
    for mapping in workflow.mappings:
        apply_parameter_defaults(mapping)


def apply_parameter_defaults(mapping: ParameterMapping) -> None:
    """Apply default values to a mapping and contained params if they're not set already."""

    if not mapping.actual_value_set and mapping.default_value:
        mapping.actual_value = mapping.default_value

        for mapped_to in mapping.mapped_parameters:
            if isinstance(mapped_to, ParameterMapping):
                apply_parameter_defaults(mapped_to)
            else:
                if not mapped_to.actual_value_set:
                    mapped_to.actual_value = mapping.default_value


def apply_parameters_values(workflow: GroupedRun, values: Dict[str, str], override: bool = False) -> None:
    """Apply values to mappings of a GroupedRun."""
    for k, v in values.items():
        mapping = next((m for m in workflow.mappings if m.name == k), None)

        if not mapping:
            raise errors.ParameterNotFoundError(k, workflow.name)

        apply_value_recursively(mapping, v, override)


def apply_value_recursively(mapping: ParameterMapping, value: Any, override: bool = False) -> None:
    """Apply a value to a ParameterMapping's children recursively."""

    if override or not mapping.actual_value_set:
        mapping.actual_value = value

    for m in mapping.mapped_parameters:
        if override or not m.actual_value_set:
            m.actual_value = value

        if isinstance(m, ParameterMapping):
            apply_value_recursively(m, value, override)
