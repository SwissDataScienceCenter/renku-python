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
"""Tests for Run API."""

from pathlib import Path

import yaml

from renku.api import Input, Output, Parameter, Project
from renku.core.management.workflow.plan_factory import (
    get_indirect_inputs_path,
    get_indirect_outputs_path,
    read_indirect_parameters,
)


def test_indirect_inputs(client):
    """Test defining indirect inputs."""
    path_1 = "/some/absolute/path"
    path_2 = "relative/path"
    path_3 = "a/path with white-spaces"

    input_1 = Input("input-1", path_1)

    with Project() as project:
        input_2 = Input("input-2", path_2)

    input_3 = Input("input-3", path_3)

    assert Path(path_1) == input_1.path
    assert Path(path_2) == input_2.path
    assert Path(path_3) == input_3.path

    content = get_indirect_inputs_path(project.path).read_text()

    assert {path_1, path_2, path_3} == set(yaml.safe_load(content).values())
    assert {input_1.name, input_2.name, input_3.name} == set(yaml.safe_load(content).keys())


def test_indirect_outputs(client):
    """Test defining indirect outputs."""
    path_1 = "/some/absolute/path"
    path_2 = "relative/path"
    path_3 = "a/path with white-spaces"

    output_1 = Output("output-1", path_1)

    with Project() as project:
        output_2 = Output("output-2", path_2)

    output_3 = Output("output-3", path_3)

    assert Path(path_1) == output_1.path
    assert Path(path_2) == output_2.path
    assert Path(path_3) == output_3.path

    content = get_indirect_outputs_path(project.path).read_text()

    assert {path_1, path_2, path_3} == set(yaml.safe_load(content).values())
    assert {output_1.name, output_2.name, output_3.name} == set(yaml.safe_load(content).keys())


def test_indirect_inputs_outputs(client):
    """Test defining indirect inputs and outputs together."""
    path_1 = "/some/absolute/path"
    path_2 = "relative/path"

    input_1 = Input("input-1", path_1)
    output_2 = Output("output-1", path_2)

    assert Path(path_1) == input_1.path
    assert Path(path_2) == output_2.path

    input_content = get_indirect_inputs_path(client.path).read_text()
    output_content = get_indirect_outputs_path(client.path).read_text()

    assert path_1 == list(yaml.safe_load(input_content).values())[0]
    assert input_1.name == list(yaml.safe_load(input_content).keys())[0]
    assert path_2 == list(yaml.safe_load(output_content).values())[0]
    assert output_2.name == list(yaml.safe_load(output_content).keys())[0]


def test_open_inputs(client):
    """Test inputs can be passed to open function."""
    with open(Input("input-1", "input.txt"), "w") as f:
        f.write("some data")

    assert "some data" == (client.path / "input.txt").read_text()


def test_open_outputs(client):
    """Test outputs can be passed to open function."""
    with open(Output("output-1", "output.txt"), "w") as f:
        f.write("some data")

    assert "some data" == (client.path / "output.txt").read_text()


def test_parameters(client):
    """Test defining parameters."""
    p1 = Parameter("parameter 1", 42)

    with Project():
        p2 = Parameter("param-2", "42")

    p3 = Parameter("parameter_3 ", 42.42)

    assert (42, "42", 42.42) == (p1, p2, p3)

    data = read_indirect_parameters(client.path)

    assert {"parameter 1", "param-2", "parameter_3 "} == set(data.keys())
    assert {42, "42", 42.42} == set(data.values())
