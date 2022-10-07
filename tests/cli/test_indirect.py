# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Test behavior of indirect inputs/outputs files list."""

from pathlib import Path

import pytest

from renku.core.util.contexts import chdir


def test_indirect_inputs_outputs(renku_cli, project):
    """Test indirect inputs/outputs that are programmatically created."""
    with chdir(project.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()
        Path("baz").touch()
        Path("qux").touch()

        Path("script.sh").write_text(
            """
            echo 'input-1: foo' >> .renku/tmp/inputs.yml
            echo 'input-2: baz' >> .renku/tmp/inputs.yml
            echo 'output-3: qux' >> .renku/tmp/outputs.yml
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    exit_code, activity = renku_cli("run", "sh", "-c", "sh script.sh")

    plan = activity.association.plan
    assert 0 == exit_code
    assert 2 == len(plan.inputs)
    assert 1 == len(plan.parameters)
    plan.inputs.sort(key=lambda e: e.default_value)
    assert "baz" == str(plan.inputs[0].default_value)
    assert plan.inputs[0].position is None
    assert "foo" == str(plan.inputs[1].default_value)
    assert plan.inputs[1].position is None

    assert 1 == len(plan.outputs)
    assert "qux" == plan.outputs[0].default_value


def test_duplicate_indirect_inputs(renku_cli, project):
    """Test duplicate indirect inputs are only included once."""
    with chdir(project.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()
        Path("baz").touch()

        Path("script.sh").write_text(
            """
            echo 'input-1: foo/bar' >> .renku/tmp/inputs.yml
            echo 'input-1: foo/bar' >> .renku/tmp/inputs.yml
            echo 'input-1: foo/./bar' >> .renku/tmp/inputs.yml
            echo 'input-1: foo/../foo/bar' >> .renku/tmp/inputs.yml
            echo 'input-2: baz' >> .renku/tmp/inputs.yml
            echo 'input-2: baz' >> .renku/tmp/inputs.yml
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    exit_code, activity = renku_cli("run", "--no-output", "sh", "-c", "sh script.sh", "baz")

    assert 0 == exit_code
    assert {"baz", "foo/bar"} == {i.default_value for i in activity.association.plan.inputs}


def test_duplicate_indirect_outputs(renku_cli, project):
    """Test duplicate indirect outputs are only included once."""
    with chdir(project.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()

        Path("script.sh").write_text(
            """
            echo 'output-1: foo/bar' >> .renku/tmp/outputs.yml
            echo 'output-1: foo/bar' >> .renku/tmp/outputs.yml
            echo 'output-1: foo/./bar' >> .renku/tmp/outputs.yml
            echo 'output-1: foo/../foo/bar' >> .renku/tmp/outputs.yml
            echo 'output-2: baz' >> .renku/tmp/outputs.yml
            echo 'output-2: baz' >> .renku/tmp/outputs.yml

            touch baz
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    exit_code, activity = renku_cli("run", "sh", "-c", "sh script.sh")

    assert 0 == exit_code
    assert {"baz", "foo/bar"} == {o.default_value for o in activity.association.plan.outputs}


def test_indirect_parameters(renku_cli, project):
    """Test indirect parameters."""
    with chdir(project.path):
        Path(".renku/tmp").mkdir()

        Path("script.sh").write_text(
            """
            echo param 1: 42 >> .renku/tmp/parameters.yml
            echo param-2: 42.42 >> .renku/tmp/parameters.yml
            echo param3: 42 >> .renku/tmp/parameters.yml
            echo param 1: "forty-two" >> .renku/tmp/parameters.yml
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    exit_code, activity = renku_cli("run", "--no-output", "sh", "-c", "sh script.sh")

    plan = activity.association.plan
    assert 0 == exit_code
    assert {"c-1", "param 1", "param-2", "param3"} == {a.name for a in plan.parameters}
    assert {"sh script.sh", "forty-two", "42.42", "42"} == {a.default_value for a in plan.parameters}

    param_1 = next(p for p in plan.parameters if p.name == "param 1")
    assert " " not in param_1.id


@pytest.mark.skip("renku update is not implemented with new database, reenable once it is.")
def test_indirect_parameters_update(renku_cli, project):
    """Test updating of indirect parameters."""
    with chdir(project.path):
        Path(".renku/tmp").mkdir(exist_ok=True)

        Path("script.sh").write_text(
            """
            echo param 1: "forty-two" >> .renku/tmp/parameters.yml
            echo param-2: 42.42 >> .renku/tmp/parameters.yml
            echo param3: 42 >> .renku/tmp/parameters.yml
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    renku_cli("run", "sh", "script.sh", stdout="result")

    with chdir(project.path):
        Path(".renku/tmp").mkdir(exist_ok=True)

        Path("script.sh").write_text(
            """
            echo param 1: "forty-two-updated" >> .renku/tmp/parameters.yml
            echo param-2: 42.42 >> .renku/tmp/parameters.yml
            """
        )

        project.repository.add(all=True)
        project.repository.commit("test setup")

    exit_code, activity = renku_cli("update", "--all")

    assert 0 == exit_code
    assert {"forty-two-updated", "42.42", "42"} == {a.default_value for a in activity.association.plan.parameters}
