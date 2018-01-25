# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

from renga._compat import Path
from renga.models.cwl.command_line_tool import CommandLineTool


def test_1st_tool():
    """Check creation of 1st tool example from args."""
    tool = CommandLineTool.from_args(('echo', 'Hello world!'))
    assert tool.cwlVersion == 'v1.0'
    assert tool.__class__.__name__ == 'CommandLineTool'
    assert tool.inputs[0].default == 'Hello world!'


def test_03_input(instance_path):
    """Check the essential input parameters."""
    whale = Path(instance_path) / 'whale.txt'
    whale.touch()

    tool = CommandLineTool.from_args((
        'echo',
        '-f',
        '-i42',
        '--example-string',
        'hello',
        '--file=whale.txt',
    ), directory=instance_path)

    assert tool.arguments[0].prefix == '-f'

    assert tool.inputs[0].default == 42
    assert tool.inputs[0].type == 'integer'
    assert tool.inputs[0].inputBinding.prefix == '-i'
    assert tool.inputs[0].inputBinding.separate is False

    assert tool.inputs[1].default == 'hello'
    assert tool.inputs[1].type == 'string'
    assert tool.inputs[1].inputBinding.prefix == '--example-string'
    assert tool.inputs[1].inputBinding.separate is True

    assert tool.inputs[2].default == 'whale.txt'
    assert tool.inputs[2].type == 'File'
    assert tool.inputs[2].inputBinding.prefix == '--file='
    assert tool.inputs[2].inputBinding.separate is False


def test_base_command_detection(instance_path):
    """Test base command detection."""
    whale = Path(instance_path) / 'hello.tar'
    whale.touch()

    tool = CommandLineTool.from_args(('tar', 'xf', 'hello.tar'),
                                     directory=instance_path)

    assert tool.baseCommand == ['tar', 'xf']
    assert tool.inputs[0].default == 'hello.tar'
    assert tool.inputs[0].type == 'File'
    assert tool.inputs[0].inputBinding.prefix is None
    assert tool.inputs[0].inputBinding.separate is True
