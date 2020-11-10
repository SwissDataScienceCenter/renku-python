# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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

from renku.core.models.entities import Collection, Entity
from renku.core.utils.contexts import chdir


def test_indirect_inputs_outputs(renku_cli, client):
    """Test indirect inputs/outputs that are programmatically created."""
    with chdir(client.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()
        Path("baz").touch()
        Path("qux").touch()

        Path("script.sh").write_text(
            """
            echo foo >> .renku/tmp/inputs.txt
            echo baz >> .renku/tmp/inputs.txt
            echo qux >> .renku/tmp/outputs.txt
            """
        )

        client.repo.git.add("--all")
        client.repo.index.commit("test setup")

    exit_code, plan = renku_cli("run", "sh", "-c", "sh script.sh")

    assert 0 == exit_code
    assert 2 == len(plan.inputs)
    assert 1 == len(plan.arguments)
    plan.inputs.sort(key=lambda e: e.consumes.path)
    assert "baz" == str(plan.inputs[0].consumes.path)
    assert isinstance(plan.inputs[0].consumes, Entity)
    assert plan.inputs[0].position is None
    assert "foo" == str(plan.inputs[1].consumes.path)
    assert isinstance(plan.inputs[1].consumes, Collection)
    assert plan.inputs[1].position is None

    assert 1 == len(plan.outputs)
    assert "qux" == plan.outputs[0].produces.path


def test_duplicate_indirect_inputs(renku_cli, client):
    """Test duplicate indirect inputs are only included once."""
    with chdir(client.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()
        Path("baz").touch()

        Path("script.sh").write_text(
            """
            echo foo/bar >> .renku/tmp/inputs.txt
            echo foo/bar >> .renku/tmp/inputs.txt
            echo foo/./bar >> .renku/tmp/inputs.txt
            echo foo/../foo/bar >> .renku/tmp/inputs.txt
            echo baz >> .renku/tmp/inputs.txt
            echo baz >> .renku/tmp/inputs.txt
            """
        )

        client.repo.git.add("--all")
        client.repo.index.commit("test setup")

    exit_code, plan = renku_cli("run", "--no-output", "sh", "-c", "sh script.sh", "baz")

    assert 0 == exit_code
    assert {"baz", "foo/bar"} == {i.consumes.path for i in plan.inputs}


def test_duplicate_indirect_outputs(renku_cli, client):
    """Test duplicate indirect outputs are only included once."""
    with chdir(client.path):
        Path("foo").mkdir()
        Path(".renku/tmp").mkdir()
        Path("foo/bar").touch()

        Path("script.sh").write_text(
            """
            echo foo/bar >> .renku/tmp/outputs.txt
            echo foo/bar >> .renku/tmp/outputs.txt
            echo foo/./bar >> .renku/tmp/outputs.txt
            echo foo/../foo/bar >> .renku/tmp/outputs.txt
            echo baz >> .renku/tmp/outputs.txt
            echo baz >> .renku/tmp/outputs.txt

            touch baz
            """
        )

        client.repo.git.add("--all")
        client.repo.index.commit("test setup")

    exit_code, plan = renku_cli("run", "sh", "-c", "sh script.sh")

    assert 0 == exit_code
    assert {"baz", "foo/bar"} == {o.produces.path for o in plan.outputs}
