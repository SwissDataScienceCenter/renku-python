# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Renku workflow commands tests."""


from contextlib import nullcontext

import pytest

from renku.core import errors
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.value_resolution import CompositePlanValueResolver
from renku.core.models.workflow.composite_plan import CompositePlan


def _get_nested_actual_values(run):
    """Get a dict of parameter -> actual values mappings from a run."""
    result = dict()
    if isinstance(run, CompositePlan):
        for step in run.plans:
            result[step.name] = _get_nested_actual_values(step)
    else:
        for param in run.inputs + run.outputs + run.parameters:
            result[param.name] = param.actual_value

    return result


def test_composite_plan_resolve_relative_mapping(composite_plan):
    """Test if resolving a relative command parameter mapping works."""

    grouped, run1, run2 = composite_plan

    assert grouped.resolve_mapping_path("@step1.@input1")[0] == run1.inputs[0]
    assert grouped.resolve_mapping_path("@step1.@input2")[0] == run1.inputs[1]
    assert grouped.resolve_mapping_path("@step1.@output1")[0] == run1.outputs[0]
    assert grouped.resolve_mapping_path("@step1.@output2")[0] == run1.outputs[1]
    assert grouped.resolve_mapping_path("@step1.@param1")[0] == run1.parameters[0]
    assert grouped.resolve_mapping_path("@step1.@param2")[0] == run1.parameters[1]

    assert grouped.resolve_mapping_path("@step2.@input1")[0] == run2.inputs[0]
    assert grouped.resolve_mapping_path("@step2.@input2")[0] == run2.inputs[1]
    assert grouped.resolve_mapping_path("@step2.@output1")[0] == run2.outputs[0]
    assert grouped.resolve_mapping_path("@step2.@output2")[0] == run2.outputs[1]
    assert grouped.resolve_mapping_path("@step2.@param1")[0] == run2.parameters[0]
    assert grouped.resolve_mapping_path("@step2.@param2")[0] == run2.parameters[1]


def test_composite_plan_resolve_absolute_mapping(composite_plan):
    """Test if resolving an absolute command parameter mapping works."""

    grouped, run1, run2 = composite_plan

    assert grouped.resolve_mapping_path("run1.run1_input1")[0] == run1.inputs[0]
    assert grouped.resolve_mapping_path("run1.run1_input2")[0] == run1.inputs[1]
    assert grouped.resolve_mapping_path("run1.run1_output1")[0] == run1.outputs[0]
    assert grouped.resolve_mapping_path("run1.run1_output2")[0] == run1.outputs[1]
    assert grouped.resolve_mapping_path("run1.run1_param1")[0] == run1.parameters[0]
    assert grouped.resolve_mapping_path("run1.run1_param2")[0] == run1.parameters[1]

    assert grouped.resolve_mapping_path("run2.run2_input1")[0] == run2.inputs[0]
    assert grouped.resolve_mapping_path("run2.run2_input2")[0] == run2.inputs[1]
    assert grouped.resolve_mapping_path("run2.run2_output1")[0] == run2.outputs[0]
    assert grouped.resolve_mapping_path("run2.run2_output2")[0] == run2.outputs[1]
    assert grouped.resolve_mapping_path("run2.run2_param1")[0] == run2.parameters[0]
    assert grouped.resolve_mapping_path("run2.run2_param2")[0] == run2.parameters[1]


@pytest.mark.parametrize(
    "mappings, names",
    [
        (["prop1=@step1.@input1"], {"prop1"}),
        (
            ["prop_abcdefg123A=run2.run2_output1", "prop47=run2.@output2", "something=@step1.run1_param1"],
            {"prop_abcdefg123A", "prop47", "something"},
        ),
        (["prop1=@step1.@input1", "prop2=prop1"], {"prop1", "prop2"}),
    ],
)
def test_composite_plan_create_mapping(composite_plan, mappings, names):
    """Test parsing of mapping strings."""
    grouped, _, _ = composite_plan

    grouped.set_mappings_from_strings(mappings)

    created_mappings = {m.name for m in grouped.mappings}

    assert created_mappings == names


def test_composite_plan_create_mapping_nested(composite_plan):
    """Test parsing of mapping strings."""
    grouped, run1, _ = composite_plan

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=@step2.@output2"])

    grouped2 = CompositePlan(id=CompositePlan.generate_id(), plans=[run1, grouped], name="grouped2")

    with pytest.raises(errors.MappingExistsError):
        grouped2.set_mappings_from_strings(["prop2=grouped1.@mapping1", "prop3=grouped1.prop1"])

    grouped2.set_mappings_from_strings(["prop3=grouped1.prop2", "direct=run1.@input1", "nested=grouped1.run1.@input2"])

    assert len(grouped2.mappings) == 4

    assert next(filter(lambda m: m.name == "prop2", grouped2.mappings)).mapped_parameters[0] == grouped.mappings[0]
    assert next(filter(lambda m: m.name == "prop3", grouped2.mappings)).mapped_parameters[0] == grouped.mappings[1]
    assert next(filter(lambda m: m.name == "direct", grouped2.mappings)).mapped_parameters[0] == run1.inputs[0]
    assert next(filter(lambda m: m.name == "nested", grouped2.mappings)).mapped_parameters[0] == run1.inputs[1]


def test_composite_plan_set_defaults(composite_plan):
    """Test setting default values on grouping runs."""

    grouped, run1, run2 = composite_plan

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    grouped.set_mapping_defaults(["prop1=1", "prop2=abcdefg", "@step1.@param1=xyz", "run2.run2_input1=7.89"])

    assert next(filter(lambda m: m.name == "prop1", grouped.mappings)).default_value == "1"
    assert next(filter(lambda m: m.name == "prop2", grouped.mappings)).default_value == "abcdefg"
    assert run1.parameters[0].default_value == "xyz"
    assert run2.inputs[0].default_value == "7.89"

    assert run1.inputs[0].default_value != "1"
    assert run2.outputs[1].default_value != "abcdefg"


def test_composite_plan_set_description(composite_plan):
    """Test setting description on grouping runs."""

    grouped, run1, run2 = composite_plan

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    grouped.set_mapping_descriptions(['prop1="first property"', 'prop2="another property?"'])

    assert next(filter(lambda m: m.name == "prop1", grouped.mappings)).description == "first property"
    assert next(filter(lambda m: m.name == "prop2", grouped.mappings)).description == "another property?"

    with pytest.raises(errors.ParameterNotFoundError):
        grouped.set_mapping_descriptions(["@step1.@param1=xyz", "run2.run2_input1=7.89"])


def test_composite_plan_map_all_inputs(composite_plan):
    """Test automatic mapping of all child inputs."""

    grouped, run1, run2 = composite_plan

    grouped.map_all_inputs()
    assert len(grouped.mappings) == 4
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.inputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.inputs[1])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.inputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.inputs[1])


def test_composite_plan_map_all_outputs(composite_plan):
    """Test automatic mapping of all child outputs."""

    grouped, run1, run2 = composite_plan

    grouped.map_all_outputs()
    assert len(grouped.mappings) == 4
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.outputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.outputs[1])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.outputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.outputs[1])


def test_composite_plan_map_all_parameters(composite_plan):
    """Test automatic mapping of all child parameters."""

    grouped, run1, run2 = composite_plan

    grouped.map_all_parameters()
    assert len(grouped.mappings) == 4
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.parameters[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.parameters[1])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.parameters[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.parameters[1])


@pytest.mark.parametrize(
    "mappings, defaults, values, expected",
    [
        # NOTE: Test without anything
        (
            [],
            [],
            {},
            {
                "run1": {
                    "run1_input1": 1,
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": 6,
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": 2,
                    "run2_output1": 3,
                    "run2_output2": 4,
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with values applied
        (
            [],
            [],
            {
                "steps": {
                    "run1": {
                        "run1_param2": "f",
                    },
                    "run2": {
                        "run2_input2": "h",
                    },
                }
            },
            {
                "run1": {
                    "run1_input1": 1,
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": "f",
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": "h",
                    "run2_output1": 3,
                    "run2_output2": 4,
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with mappings
        (
            ["m1=@step1.@input1", "m2=@step2.@output2"],
            [],
            {},
            {
                "run1": {
                    "run1_input1": 1,
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": 6,
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": 2,
                    "run2_output1": 3,
                    "run2_output2": 4,
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with mappings and defaults
        (
            ["m1=@step1.@input1", "m2=@step2.@output2"],
            ["m1=x", "m2=y"],
            {},
            {
                "run1": {
                    "run1_input1": "x",
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": 6,
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": 2,
                    "run2_output1": 3,
                    "run2_output2": "y",
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with mappings, defaults and values
        (
            ["m1=@step1.@input1", "m2=@step2.@output2"],
            ["m1=x", "m2=y"],
            {
                "steps": {
                    "run1": {
                        "run1_param2": "f",
                    },
                    "run2": {"run2_input2": "h", "run2_output2": "a"},  # NOTE: Override value provided by mapping
                }
            },
            {
                "run1": {
                    "run1_input1": "x",
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": "f",
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": "h",
                    "run2_output1": 3,
                    "run2_output2": "a",
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with mappings, defaults and values for mappings and params
        (
            ["m1=@step1.@input1", "m2=@step2.@output2"],
            ["m1=x", "m2=y"],
            {
                "parameters": {"m2": "z"},
                "steps": {
                    "run1": {
                        "run1_param2": "f",
                    },
                    "run2": {"run2_input2": "h"},  # NOTE: Override value provided by mapping
                },
            },
            {
                "run1": {
                    "run1_input1": "x",
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": "f",
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": "h",
                    "run2_output1": 3,
                    "run2_output2": "z",
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
        # NOTE: Test with mappings, defaults and values for mappings and params
        (
            ["m1=@step1.@input1", "m2=@step2.@output2"],
            ["m1=x", "m2=y"],
            {
                "parameters": {"m1": "42"},
                "steps": {
                    "run1": {
                        "run1_param2": "f",
                    },
                    "run2": {"run2_input2": "h", "run2_output2": "a"},  # NOTE: Override value provided by mapping
                },
            },
            {
                "run1": {
                    "run1_input1": "42",
                    "run1_input2": 2,
                    "run1_output1": 3,
                    "run1_output2": 4,
                    "run1_param1": 5,
                    "run1_param2": "f",
                },
                "run2": {
                    "run2_input1": 1,
                    "run2_input2": "h",
                    "run2_output1": 3,
                    "run2_output2": "a",
                    "run2_param1": 5,
                    "run2_param2": 6,
                },
            },
        ),
    ],
)
def test_composite_plan_actual_values(composite_plan, mappings, defaults, values, expected):
    """Test resolving actual values on a grouped run."""

    grouped, _, _ = composite_plan

    grouped.set_mappings_from_strings(mappings)
    grouped.set_mapping_defaults(defaults)
    rv = CompositePlanValueResolver(grouped, values)

    actual = _get_nested_actual_values(rv.apply())
    assert len(rv.missing_parameters) == 0

    assert actual == expected


@pytest.mark.parametrize(
    "links,raises,cycles",
    [
        ([], False, False),
        (["@mapping1=@mapping2"], True, False),
        (["@mapping1=@step1.@input1"], True, False),
        (["@step1.@input1=@step2.@output2"], True, False),
        (["@step2.@output2=@step1.@input1"], False, False),
        (["@step2.@output2=@step1.@input1", "@step2.@output1=@step1.@input1"], True, False),
        (["@step2.@output2=@step1.@input1", "@step1.@output1=@step2.@input1"], False, True),
    ],
)
def test_composite_plan_links(composite_plan, links, raises, cycles):
    """Test adding links to grouped runs."""

    grouped, _, _ = composite_plan
    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    if raises:
        with pytest.raises(errors.ParameterLinkError):
            grouped.set_links_from_strings(links)
    else:
        grouped.set_links_from_strings(links)
        found_cycles = ExecutionGraph(grouped).cycles
        assert bool(found_cycles) == cycles, found_cycles


@pytest.mark.parametrize(
    "mappings,defaults,links,raises,cycles",
    [
        ([], [], False, False, False),
        ([], ["@step1.@output1=myfile.txt", "@step2.@input1=myfile.txt"], True, False, False),
        (
            [],
            ["@step1.@output1=myfile.txt", "@step2.@input1=myfile.txt", "@step1.@input1=myfile.txt"],
            True,
            True,
            True,
        ),
        (
            [],
            [
                "@step1.@output1=myfile.txt",
                "@step2.@input1=myfile.txt",
                "@step1.@input1=otherfile.txt",
                "@step2.@output2=otherfile.txt",
            ],
            True,
            False,
            True,
        ),
        (
            ["input=@step1.@input1", "output=@step2.@output2"],
            ["input=myfile.txt", "output=myfile.txt"],
            True,
            False,
            False,
        ),
        (
            ["input=@step1.@input1,@step2.@input2", "output=@step2.@output2"],
            ["input=myfile.txt", "output=myfile.txt"],
            True,
            True,
            True,
        ),
        (["map=@step1.@input1,@step2.@input2,@step1.@output1,@step2.@output2"], ["map=myfile.txt"], True, True, True),
    ],
)
def test_composite_plan_auto_links(composite_plan, mappings, defaults, links, raises, cycles):
    """Test automatically detecting links between steps."""
    grouped, _, _ = composite_plan

    if raises:
        maybe_raises = pytest.raises(errors.ParameterLinkError)
    else:
        maybe_raises = nullcontext()

    grouped.set_mappings_from_strings(mappings)
    grouped.set_mapping_defaults(defaults)

    rv = CompositePlanValueResolver(grouped, None)
    grouped = rv.apply()
    assert len(rv.missing_parameters) == 0

    graph = ExecutionGraph(grouped, virtual_links=True)

    assert bool(graph.virtual_links) == links
    assert bool(graph.cycles) == cycles

    with maybe_raises:
        for virtual_link in graph.virtual_links:
            grouped.add_link(virtual_link[0], [virtual_link[1]])
