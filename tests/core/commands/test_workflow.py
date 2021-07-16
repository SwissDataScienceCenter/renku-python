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


import pytest

from renku.core import errors
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.models.workflow.grouped_run import GroupedRun


def _get_nested_actual_values(run):
    """Get a dict of parameter -> actual values mappings from a run."""
    result = dict()
    if isinstance(run, GroupedRun):
        for step in run.plans:
            result[step.name] = _get_nested_actual_values(step)
    else:
        for param in run.inputs + run.outputs + run.parameters:
            result[param.name] = param.actual_value

    return result


def test_grouped_run_resolve_relative_mapping(grouped_run):
    """Test if resolving a relative command parameter mapping works."""

    grouped, run1, run2 = grouped_run

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


def test_grouped_run_resolve_absolute_mapping(grouped_run):
    """Test if resolving an absolute command parameter mapping works."""

    grouped, run1, run2 = grouped_run

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
def test_grouped_run_create_mapping(grouped_run, mappings, names):
    """Test parsing of mapping strings."""
    grouped, _, _ = grouped_run

    grouped.set_mappings_from_strings(mappings)

    created_mappings = {m.name for m in grouped.mappings}

    assert created_mappings == names


def test_grouped_run_create_mapping_nested(grouped_run):
    """Test parsing of mapping strings."""
    grouped, run1, _ = grouped_run

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=@step2.@output2"])

    grouped2 = GroupedRun(id=GroupedRun.generate_id(), plans=[run1, grouped], name="grouped2")

    with pytest.raises(errors.MappingExistsError):
        grouped2.set_mappings_from_strings(["prop2=grouped1.@mapping1", "prop3=grouped1.prop1"])

    grouped2.set_mappings_from_strings(["prop3=grouped1.prop2", "direct=run1.@input1", "nested=grouped1.run1.@input2"])

    assert len(grouped2.mappings) == 4

    assert next(filter(lambda m: m.name == "prop2", grouped2.mappings)).mapped_parameters[0] == grouped.mappings[0]
    assert next(filter(lambda m: m.name == "prop3", grouped2.mappings)).mapped_parameters[0] == grouped.mappings[1]
    assert next(filter(lambda m: m.name == "direct", grouped2.mappings)).mapped_parameters[0] == run1.inputs[0]
    assert next(filter(lambda m: m.name == "nested", grouped2.mappings)).mapped_parameters[0] == run1.inputs[1]


def test_grouped_run_set_defaults(grouped_run):
    """Test setting default values on grouping runs."""

    grouped, run1, run2 = grouped_run

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    grouped.set_mapping_defaults(["prop1=1", "prop2=abcdefg", "@step1.@param1=xyz", "run2.run2_input1=7.89"])

    assert next(filter(lambda m: m.name == "prop1", grouped.mappings)).default_value == "1"
    assert next(filter(lambda m: m.name == "prop2", grouped.mappings)).default_value == "abcdefg"
    assert run1.parameters[0].default_value == "xyz"
    assert run2.inputs[0].default_value == "7.89"

    assert run1.inputs[0].default_value != "1"
    assert run2.outputs[1].default_value != "abcdefg"


def test_grouped_run_set_description(grouped_run):
    """Test setting description on grouping runs."""

    grouped, run1, run2 = grouped_run

    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    grouped.set_mapping_descriptions(['prop1="first property"', 'prop2="another property?"'])

    assert next(filter(lambda m: m.name == "prop1", grouped.mappings)).description == "first property"
    assert next(filter(lambda m: m.name == "prop2", grouped.mappings)).description == "another property?"

    with pytest.raises(errors.ParameterNotFoundError):
        grouped.set_mapping_descriptions(["@step1.@param1=xyz", "run2.run2_input1=7.89"])


def test_grouped_run_map_all_inputs(grouped_run):
    """Test automatic mapping of all child inputs."""

    grouped, run1, run2 = grouped_run

    grouped.map_all_inputs()
    assert len(grouped.mappings) == 4
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.inputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.inputs[1])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.inputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.inputs[1])


def test_grouped_run_map_all_outputs(grouped_run):
    """Test automatic mapping of all child outputs."""

    grouped, run1, run2 = grouped_run

    grouped.map_all_outputs()
    assert len(grouped.mappings) == 4
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.outputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run1.outputs[1])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.outputs[0])
    assert any(m for m in grouped.mappings if m.mapped_parameters[0] == run2.outputs[1])


def test_grouped_run_map_all_parameters(grouped_run):
    """Test automatic mapping of all child parameters."""

    grouped, run1, run2 = grouped_run

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
def test_grouped_run_actual_values(grouped_run, mappings, defaults, values, expected):
    """Test resolving actual values on a grouped run."""
    from renku.core.management.workflow.value_resolution import apply_run_values

    grouped, _, _ = grouped_run

    grouped.set_mappings_from_strings(mappings)
    grouped.set_mapping_defaults(defaults)
    apply_run_values(grouped, values)

    actual = _get_nested_actual_values(grouped)

    assert actual == expected


@pytest.mark.parametrize(
    "links,raises,cycles",
    [
        ([], False, False),
        (["@mapping1=@mapping2"], True, False),
        (["@mapping1=@step1.@input1"], True, False),
        (["@step1.@input1=@step2.@output2"], True, False),
        (["@step2.@output2=@step1.@input1"], False, False),
        (["@step2.@output2=@step1.@input1", "@step2.@output1=@step1.@input1"], False, False),
        (["@step2.@output2=@step1.@input1", "@step1.@output1=@step2.@input1"], False, True),
    ],
)
def test_grouped_run_links(grouped_run, links, raises, cycles):
    """Test adding links to grouped runs."""

    grouped, _, _ = grouped_run
    grouped.set_mappings_from_strings(["prop1=@step1.@input1", "prop2=run2.@output2"])

    if raises:
        with pytest.raises(errors.ParameterLinkError):
            grouped.set_links_from_strings(links)
    else:
        grouped.set_links_from_strings(links)
        found_cycles = ExecutionGraph(grouped).cycles
        assert bool(found_cycles) == cycles, found_cycles
