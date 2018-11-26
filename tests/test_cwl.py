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

import pytest
import yaml

from renku._compat import Path
from renku.models.cwl import CWLClass
from renku.models.cwl.command_line_tool import CommandLineToolFactory


def test_1st_tool():
    """Check creation of 1st tool example from args."""
    tool = CommandLineToolFactory(('echo', 'Hello world!')).generate_tool()
    assert tool.cwlVersion == 'v1.0'
    assert tool.__class__.__name__ == 'CommandLineTool'
    assert tool.inputs[0].default == 'Hello world!'


def test_03_input(instance_path):
    """Check the essential input parameters."""
    whale = Path(instance_path) / 'whale.txt'
    whale.touch()

    argv = [
        'echo',
        '-f',
        '-i42',
        '--example-string',
        'hello',
        '--file=whale.txt',
    ]
    tool = CommandLineToolFactory(
        argv, directory=instance_path, working_dir=instance_path
    ).generate_tool()

    assert tool.arguments[0].to_argv() == ['-f']

    assert tool.inputs[0].default == 42
    assert tool.inputs[0].type == 'int'
    assert tool.inputs[0].inputBinding.prefix == '-i'
    assert tool.inputs[0].inputBinding.separate is False

    assert tool.inputs[1].default == 'hello'
    assert tool.inputs[1].type == 'string'
    assert tool.inputs[1].inputBinding.prefix == '--example-string'
    assert tool.inputs[1].inputBinding.separate is True

    assert tool.inputs[2].default.path.samefile(whale)
    assert tool.inputs[2].type == 'File'
    assert tool.inputs[2].inputBinding.prefix == '--file='
    assert tool.inputs[2].inputBinding.separate is False

    assert tool.to_argv() == argv


def test_base_command_detection(instance_path):
    """Test base command detection."""
    hello = Path(instance_path) / 'hello.tar'
    hello.touch()

    argv = ['tar', 'xf', 'hello.tar']
    tool = CommandLineToolFactory(
        argv, directory=instance_path, working_dir=instance_path
    ).generate_tool()

    assert tool.baseCommand == ['tar', 'xf']
    assert tool.inputs[0].default.path.samefile(hello)
    assert tool.inputs[0].type == 'File'
    assert tool.inputs[0].inputBinding.prefix is None
    assert tool.inputs[0].inputBinding.separate is True

    assert tool.to_argv() == argv


def test_short_base_command_detection():
    """Test base command detection without arguments."""
    tool = CommandLineToolFactory(('echo', 'A')).generate_tool()
    assert tool.cwlVersion == 'v1.0'
    assert tool.__class__.__name__ == 'CommandLineTool'
    assert tool.inputs[0].default == 'A'

    assert tool.to_argv() == ['echo', 'A']


def test_04_output(instance_path):
    """Test describtion of outputs from a command."""
    hello = Path(instance_path) / 'hello.tar'
    hello.touch()

    argv = ['tar', 'xf', 'hello.tar']
    factory = CommandLineToolFactory(
        argv, directory=instance_path, working_dir=instance_path
    )

    # simulate run

    output = Path(instance_path) / 'hello.txt'
    output.touch()

    parameters = list(factory.guess_outputs([output]))

    assert parameters[0][0].type == 'File'
    assert parameters[0][0].outputBinding.glob == 'hello.txt'

    tool = factory.generate_tool()
    assert tool.to_argv() == argv


def test_05_stdout(instance_path):
    """Test stdout mapping."""
    output = Path(instance_path) / 'output.txt'
    output.touch()

    argv = ['echo', 'Hello world!']
    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
        stdout='output.txt',
    )

    assert factory.stdout == 'output.txt'
    assert factory.outputs[0].type == 'stdout'

    tool = factory.generate_tool()
    assert tool.to_argv() == argv


def test_stdout_with_conflicting_arg(instance_path):
    """Test stdout with conflicting argument value."""
    output = Path(instance_path) / 'lalala'
    output.touch()

    argv = ['echo', 'lalala']
    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
        stdout='lalala',
    )

    assert factory.inputs[0].default == 'lalala'
    assert factory.inputs[0].type == 'string'
    assert factory.stdout == 'lalala'
    assert factory.outputs[0].type == 'stdout'

    tool = factory.generate_tool()
    assert tool.to_argv() == argv


def test_06_params(instance_path):
    """Test referencing input parameters in other fields."""
    hello = Path(instance_path) / 'hello.tar'
    hello.touch()

    argv = ['tar', 'xf', 'hello.tar', 'goodbye.txt']
    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
    )

    assert factory.inputs[1].default == 'goodbye.txt'
    assert factory.inputs[1].type == 'string'
    assert factory.inputs[1].inputBinding.position == 2

    goodbye_id = factory.inputs[1].id

    # simulate run

    output = Path(instance_path) / 'goodbye.txt'
    output.touch()

    parameters = list(factory.guess_outputs([output]))

    assert parameters[0][0].type == 'File'
    assert parameters[0][0].outputBinding.glob == \
        '$(inputs.{0})'.format(goodbye_id)

    tool = factory.generate_tool()
    assert tool.to_argv() == argv


def test_09_array_inputs(instance_path):
    """Test specification of input parameters in arrays."""
    argv = [
        'echo',
        '-A',
        'one',
        'two',
        'three',
        '-B=four',
        '-B=five',
        '-B=six',
        '-C=seven,eight,nine',
    ]
    tool = CommandLineToolFactory(
        argv, directory=instance_path, working_dir=instance_path
    ).generate_tool()

    # TODO add grouping for -A and -B

    assert tool.inputs[-1].type == 'string[]'
    assert tool.inputs[-1].default == ['seven', 'eight', 'nine']
    assert tool.inputs[-1].inputBinding.prefix == '-C='
    assert tool.inputs[-1].inputBinding.itemSeparator == ','
    assert tool.inputs[-1].inputBinding.separate is False

    assert tool.to_argv() == argv


@pytest.mark.parametrize('argv', [['wc'], ['wc', '-l']])
def test_stdin_and_stdout(argv, instance_path):
    """Test stdout mapping."""
    input_ = Path(instance_path) / 'input.txt'
    input_.touch()
    output = Path(instance_path) / 'output.txt'
    output.touch()

    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
        stdin='input.txt',
        stdout='output.txt',
        stderr='error.log',
    )

    assert factory.stdin
    if len(argv) > 1:
        assert factory.arguments

    assert factory.stdout == 'output.txt'
    assert factory.outputs[0].type == 'stdout'

    tool = factory.generate_tool()
    assert tool.to_argv() == argv
    std_streams = ' < input.txt > output.txt 2> error.log'
    assert str(tool) == ' '.join(argv) + std_streams


def test_input_directory(instance_path):
    """Test input directory."""
    cwd = Path(instance_path)
    src = cwd / 'src'
    src.mkdir(parents=True)

    for i in range(5):
        (src / str(i)).touch()

    argv = ['tar', 'czvf', 'src.tar', 'src']
    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
    )

    src_tar = src / 'src.tar'
    src_tar.touch()

    tool = factory.generate_tool()
    assert tool.to_argv() == argv

    assert tool.inputs[0].type == 'string'
    assert tool.inputs[0].default == src_tar.name
    assert tool.inputs[1].type == 'Directory'
    assert tool.inputs[1].default.path.samefile(src)


def test_exitings_output_directory(client):
    """Test creation of InitialWorkDirRequirement for output directory."""
    instance_path = client.path
    output = client.path / 'output'

    argv = ['script', 'output']
    factory = CommandLineToolFactory(
        argv,
        directory=instance_path,
        working_dir=instance_path,
    )

    with factory.watch(client, no_output=True) as tool:
        # Script creates the directory.
        output.mkdir(parents=True)

    initial_work_dir_requirement = [
        r for r in tool.requirements
        if r.__class__.__name__ == 'InitialWorkDirRequirement'
    ]
    assert 0 == len(initial_work_dir_requirement)

    output.mkdir(parents=True, exist_ok=True)
    with factory.watch(client) as tool:
        # The directory already exists.
        (output / 'result.txt').touch()

    initial_work_dir_requirement = [
        r for r in tool.requirements
        if r.__class__.__name__ == 'InitialWorkDirRequirement'
    ]
    assert 1 == len(initial_work_dir_requirement)
    assert initial_work_dir_requirement[0].listing[0].entryname == output.name

    assert 1 == len(tool.inputs)
    assert 1 == len(tool.outputs)


LINK_CWL = """
class: CommandLineTool
cwlVersion: v1.0
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - class: Directory
        listing: $(inputs.indir.listing)
baseCommand: ["true"]
inputs:
  indir: Directory
  filename: string
outputs:
  outlist:
    type: File
    outputBinding:
      glob: $(inputs.filename)
"""


def test_load_inputs_defined_as_type():
    """Test loading of CWL definition with specific input parameters."""
    assert CWLClass.from_cwl(yaml.load(LINK_CWL))
