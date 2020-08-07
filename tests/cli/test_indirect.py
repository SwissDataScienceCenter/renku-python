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

from renku.core.models.entities import Collection, Entity


def test_indirect_inputs(renku_cli, client):
    """Test indirect inputs that are programmatically created."""
    # Set up a script that creates indirect inputs
    renku_cli("run", "--no-output", "mkdir", "foo")
    renku_cli("run", "--no-output", "mkdir", ".renku/tmp")
    renku_cli("run", "touch", "foo/bar")
    renku_cli("run", "touch", "baz")
    renku_cli("run", "touch", "qux")
    renku_cli("run", "sh", "-c", 'echo "echo foo > .renku/tmp/inputs.txt" > script.sh')
    renku_cli("run", "sh", "-c", 'echo "echo baz >> .renku/tmp/inputs.txt" >> script.sh')
    renku_cli("run", "sh", "-c", 'echo "echo qux > .renku/tmp/outputs.txt" >> script.sh')
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
