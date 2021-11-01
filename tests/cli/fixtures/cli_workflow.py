# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku CLI fixtures for workflow methods."""

import pytest


@pytest.fixture
def workflow_graph(run_shell, project):
    """Setup a project with a workflow graph."""

    def _run_workflow(name, command, extra_args=""):
        output = run_shell(f"renku run --name {name} {extra_args} -- {command}")
        # Assert not allocated stderr.
        assert output[1] is None

    _run_workflow("r1", "echo 'test' > A")
    _run_workflow("r2", "tee B C < A")
    _run_workflow("r3", "cp A Z")
    _run_workflow("r4", "cp B X")
    _run_workflow("r5", "cat C Z > Y")
    _run_workflow("r6", "bash -c 'cat X Y | tee R S'", extra_args="--input X --input Y --output R --output S")
    _run_workflow("r7", "echo 'other' > H")
    _run_workflow("r8", "tee I J < H")
