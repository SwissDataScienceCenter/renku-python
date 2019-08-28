# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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


def test_indirect_inputs(cli, client):
    """Test indirect inputs that are programmatically created."""
    # Set up a script that creates indirect inputs
    cli('run', '--no-output', 'mkdir', 'foo')
    cli('run', '--no-output', 'mkdir', '.renku/tmp')
    cli('run', 'touch', 'foo/bar')
    cli('run', 'touch', 'baz')
    cli('run', 'touch', 'qux')
    cli(
        'run', 'sh', '-c',
        'echo "echo foo > .renku/tmp/inputs.txt" > script.sh'
    )
    cli(
        'run', 'sh', '-c',
        'echo "echo baz >> .renku/tmp/inputs.txt" >> script.sh'
    )
    cli(
        'run', 'sh', '-c',
        'echo "echo qux > .renku/tmp/outputs.txt" >> script.sh'
    )

    exit_code, cwl = cli('run', 'sh', '-c', 'sh script.sh')
    assert exit_code == 0

    assert len(cwl.inputs) == 3
    cwl.inputs.sort(key=lambda e: e.type)
    assert str(cwl.inputs[0].default) == '../../foo'
    assert cwl.inputs[0].type == 'Directory'
    assert cwl.inputs[0].inputBinding is None
    assert str(cwl.inputs[1].default) == '../../baz'
    assert cwl.inputs[1].type == 'File'
    assert cwl.inputs[1].inputBinding is None

    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].outputBinding.glob == 'qux'
