# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Represent a ``CommandLineTool`` from the Common Workflow Language."""

import fnmatch
import os
import re
import shlex
import time
from contextlib import contextmanager
from pathlib import Path

import attr
import click

from renku.core import errors
from renku.core.commands.echo import INFO

from ...management.config import RENKU_HOME
from ..datastructures import DirectoryTree
from .annotation import Annotation
from .ascwl import CWLClass, mapped
from .parameter import CommandInputParameter, CommandLineBinding, \
    CommandOutputParameter
from .process import Process
from .types import PATH_OBJECTS, Directory, File

STARTED_AT = int(time.time() * 1000)

RENKU_TMP_DIR = os.path.join(RENKU_HOME, 'tmp')
RENKU_FILELIST_PATH = os.getenv('RENKU_FILELIST_PATH', RENKU_TMP_DIR)
INDIRECT_INPUTS_LIST = os.path.join(RENKU_FILELIST_PATH, 'inputs.txt')
INDIRECT_OUTPUTS_LIST = os.path.join(RENKU_FILELIST_PATH, 'outputs.txt')


def convert_arguments(value):
    """Convert arguments from various input formats."""
    if isinstance(value, (list, tuple)):
        return [
            CommandLineBinding(**item) if isinstance(item, dict) else item
            for item in value
        ]
    return shlex.split(value)


@attr.s
class CommandLineTool(Process, CWLClass):
    """Represent a command line tool."""

    STD_STREAMS_REPR = {
        'stdin': '<',
        'stdout': '>',
        'stderr': '2>',
    }
    """Format streams for a shell command representation."""

    # specialize inputs and outputs with Command{Input,Output}Parameter

    baseCommand = attr.ib(
        default='',
        validator=lambda self, attr, cmd: bool(cmd),
    )  # str or list(str) -> shutil.split()
    arguments = attr.ib(
        default=attr.Factory(list),
        converter=convert_arguments,
    )  # list(string, Expression, CommandLineBinding)

    stdin = attr.ib(default=None)
    stdout = attr.ib(default=None)
    stderr = attr.ib(default=None)

    inputs = mapped(CommandInputParameter)
    outputs = mapped(CommandOutputParameter)

    successCodes = attr.ib(default=attr.Factory(list))  # list(int)
    temporaryFailCodes = attr.ib(default=attr.Factory(list))  # list(int)
    permanentFailCodes = attr.ib(default=attr.Factory(list))  # list(int)

    annotations = attr.ib(
        metadata={
            'cwl_metadata': {
                'namespace': 'http://www.w3.org/ns/oa#',
                'prefix': 'oa',
                'property': 'oa:hasTarget',
                'reverse': True,
                'type': Annotation
            }
        },
        default=None
    )

    def _std_streams(self, basedir=None):
        """Return mapped standard streams."""
        streams = {}

        if self.stdin:
            assert self.stdin.startswith('$(inputs.')
            input_id = self.stdin.split('.')[1]
            for input_ in self.inputs:
                if input_id == input_.id:
                    streams['stdin'] = os.path.relpath(
                        str(Path(basedir or '.') / str(input_.default))
                    )
                    break
        if self.stdout:
            streams['stdout'] = self.stdout
        if self.stderr:
            streams['stderr'] = self.stderr

        return streams

    def __str__(self):
        """Generate a simple representation."""
        return ' '.join(self.to_argv()) + ' ' + ' '.join(
            self.STD_STREAMS_REPR[key] + ' ' + str(path)
            for key, path in self._std_streams().items()
        )

    def create_run(self, **kwargs):
        """Return an instance of process run."""
        from renku.core.models.provenance.activities import ProcessRun
        return ProcessRun(**kwargs)

    def get_output_id(self, path):  # pragma: no cover
        """Return an id of the matching path from default values."""
        for output in self.outputs:
            if output.type in {'stdout', 'stderr'}:
                stream = getattr(self, output.type)
                if stream == path:
                    return output.id
            elif output.type in PATH_OBJECTS:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in self.inputs:
                        if input_.id == input_id and input_.default == path:
                            return output.id
                elif fnmatch.fnmatch(path, glob):
                    return output.id

    def to_argv(self, job=None):
        """Generate arguments for system call."""
        if not isinstance(self.baseCommand, list):
            argv = [self.baseCommand]
        else:
            argv = list(self.baseCommand)

        args = [(a.position, a) for a in self.arguments]
        for i in self.inputs:
            if i.inputBinding:
                args.append((i.inputBinding.position, i))

        for _, v in sorted(args):
            argv.extend(v.to_argv())

        return argv


@attr.s
class CommandLineToolFactory(object):
    """Command Line Tool Factory."""

    _RE_SUBCOMMAND = re.compile(r'^[A-Za-z]+(-[A-Za-z]+)?$')

    command_line = attr.ib(
        converter=lambda cmd: list(cmd)
        if isinstance(cmd, (list, tuple)) else shlex.split(cmd),
    )

    explicit_inputs = attr.ib(
        default=[],
        converter=lambda paths: [Path(os.path.abspath(p)) for p in paths]
    )
    explicit_outputs = attr.ib(
        default=[],
        converter=lambda paths: [Path(os.path.abspath(p)) for p in paths]
    )

    directory = attr.ib(
        default='.',
        converter=lambda path: Path(path).resolve(),
    )
    working_dir = attr.ib(
        default='.',
        converter=lambda path: Path(path).resolve(),
    )

    stdin = attr.ib(default=None)  # null, str, Expression
    stderr = attr.ib(default=None)  # null, str, Expression
    stdout = attr.ib(default=None)  # null, str, Expression

    baseCommand = attr.ib(init=False)
    arguments = attr.ib(init=False)
    inputs = attr.ib(init=False)
    outputs = attr.ib(init=False)

    successCodes = attr.ib(default=attr.Factory(list))  # list(int)

    messages = attr.ib(default=None)
    warnings = attr.ib(default=None)

    def __attrs_post_init__(self):
        """Derive basic information."""
        self.baseCommand, detect = self.split_command_and_args()
        self.arguments = []
        self.inputs = []
        self.outputs = []

        if self.stdin:
            input_ = next(
                self.guess_inputs(str(self.working_dir / self.stdin))
            )
            assert input_.type == 'File'
            input_ = attr.evolve(
                input_,
                id='input_stdin',
                inputBinding=None,  # do not include in tool arguments
            )
            self.inputs.append(input_)
            self.stdin = '$(inputs.{0}.path)'.format(input_.id)

        for stream_name in ('stdout', 'stderr'):
            stream = getattr(self, stream_name)
            if stream and self.file_candidate(self.working_dir / stream):
                self.outputs.append(
                    CommandOutputParameter(
                        id='output_{0}'.format(stream_name),
                        type=stream_name,
                    )
                )

        for input_ in self.guess_inputs(*detect):
            if isinstance(input_, CommandLineBinding):
                self.arguments.append(input_)
            else:
                self.inputs.append(input_)

        if self.explicit_inputs:
            for input in self.find_explicit_inputs():
                self.inputs.append(input)

    def generate_tool(self):
        """Return an instance of command line tool."""
        return CommandLineTool(
            stdin=self.stdin,
            stderr=self.stderr,
            stdout=self.stdout,
            baseCommand=self.baseCommand,
            arguments=self.arguments,
            inputs=self.inputs,
            outputs=self.outputs,
            successCodes=self.successCodes,
        )

    @contextmanager
    def watch(self, client, no_output=False):
        """Watch a Renku repository for changes to detect outputs."""
        client.check_external_storage()

        tool = self.generate_tool()
        repo = client.repo

        # Remove indirect files list if any
        self.delete_indirect_files_list()

        # NOTE consider to use git index instead
        existing_directories = {
            str(p.relative_to(client.path))
            for p in client.path.glob('**/')
        }

        from renku.core.plugins.pluginmanager import get_plugin_manager
        pm = get_plugin_manager()
        pm.hook.pre_run(tool=tool)

        yield tool

        if repo:
            # Include indirect inputs and outputs before further processing
            self.add_indirect_inputs()
            self.add_indirect_outputs()
            # Remove indirect files list if any
            self.delete_indirect_files_list()

            # List of all output paths.
            paths = []

            inputs = {input.id: input for input in self.inputs}
            outputs = list(tool.outputs)

            # Keep track of unmodified output files.
            unmodified = set()

            # Calculate possible output paths.
            # Capture newly created files through redirects.
            candidates = {file_ for file_ in repo.untracked_files}

            # Capture modified files through redirects.
            candidates |= {
                o.a_path
                for o in repo.index.diff(None) if not o.deleted_file
            }

            from renku.core.commands.graph import _safe_path
            candidates = {path for path in candidates if _safe_path(path)}

            for output, input, path in self.guess_outputs(candidates):
                outputs.append(output)
                paths.append(path)

                if input is not None:
                    if input.id not in inputs:  # pragma: no cover
                        raise RuntimeError('Inconsistent input name.')

                    inputs[input.id] = input

            for stream_name in ('stdout', 'stderr'):
                stream = getattr(self, stream_name)
                if (
                    stream and stream not in candidates and
                    Path(os.path.abspath(stream)) not in self.explicit_outputs
                ):
                    unmodified.add(stream)
                elif stream:
                    paths.append(stream)

            if self.explicit_outputs:
                last_output_id = len(outputs)

                for output, input, path in self.find_explicit_outputs(
                    last_output_id
                ):
                    outputs.append(output)
                    paths.append(path)

                    if input is not None:
                        if input.id not in inputs:  # pragma: no cover
                            raise RuntimeError('Inconsistent input name.')

                        inputs[input.id] = input

            if unmodified:
                raise errors.UnmodifiedOutputs(repo, unmodified)

            if not no_output and not paths:
                raise errors.OutputsNotFound(repo, inputs.values())

            if client.check_external_storage():
                lfs_paths = client.track_paths_in_storage(*paths)

                show_message = client.get_value('renku', 'show_lfs_message')
                if (
                    lfs_paths and
                    (show_message is None or show_message == 'True')
                ):
                    self.messages = (
                        INFO + 'Adding these files to Git LFS:\n' +
                        '\t{}'.format('\n\t'.join(lfs_paths)) +
                        '\nTo disable this message in the future, run:' +
                        '\n\trenku config show_lfs_message False'
                    )

            tool.inputs = list(inputs.values())
            tool.outputs = outputs

        # Requirement detection can be done anytime.
        from .process_requirements import InitialWorkDirRequirement, \
            InlineJavascriptRequirement
        initial_work_dir_requirement = InitialWorkDirRequirement.from_tool(
            tool,
            existing_directories=existing_directories,
            working_dir=self.working_dir
        )
        if initial_work_dir_requirement:
            tool.requirements.extend([
                InlineJavascriptRequirement(),
                initial_work_dir_requirement,
            ])

        results = pm.hook.cmdline_tool_annotations(tool=tool)
        tool.annotations = [a for r in results for a in r]

    @command_line.validator
    def validate_command_line(self, attribute, value):
        """Check the command line structure."""
        if not value:
            raise errors.UsageError('Command line can not be empty.')

    @directory.validator
    def validate_path(self, attribute, value):
        """Path must exists."""
        if not value.exists():
            raise errors.UsageError('Directory must exist.')

    def file_candidate(self, candidate, ignore=None):
        """Return a path instance if it exists in current directory."""
        if ignore and candidate in ignore:
            return

        candidate = Path(candidate)

        if not candidate.is_absolute():
            candidate = self.directory / candidate

        if candidate.exists() or candidate.is_symlink():
            try:
                path = candidate.resolve()
                path.relative_to(self.directory)
            except ValueError:  # An external file
                return Path(os.path.abspath(candidate))
            else:
                return path

    def split_command_and_args(self):
        """Return tuple with command and args from command line arguments."""
        if self.file_candidate(self.command_line[0]):
            return [], list(self.command_line)

        cmd = [self.command_line[0]]
        args = list(self.command_line[1:])

        if len(args) < 2:
            # only guess subcommand for more arguments
            return cmd, args

        while args and re.match(self._RE_SUBCOMMAND, args[0]) \
                and not self.file_candidate(args[0]):
            cmd.append(args.pop(0))

        return cmd, args

    def guess_type(self, value, ignore_filenames=None):
        """Return new value and CWL parameter type."""
        candidate = self.file_candidate(value, ignore=ignore_filenames)
        if candidate:
            try:
                if candidate.is_dir():
                    return Directory(path=candidate), 'Directory', None
                return File(path=candidate), 'File', None
            except ValueError:
                # The candidate points to a file outside the working
                # directory
                # TODO suggest that the file should be imported to the repo
                pass

        try:
            value = int(value)
            return value, 'int', None
        except ValueError:
            pass

        if len(value) > 1 and ',' in value:
            return value.split(','), 'string[]', ','

        return value, 'string', None

    def guess_inputs(self, *arguments):
        """Yield command input parameters and command line bindings."""
        position = 0
        prefix = None

        output_streams = {
            getattr(self, stream_name)
            for stream_name in ('stdout', 'stderr')
        }

        for index, argument in enumerate(arguments):
            itemSeparator = None

            if prefix:
                if argument.startswith('-'):
                    position += 1
                    yield CommandLineBinding(
                        position=position,
                        valueFrom=prefix,
                    )
                    prefix = None

            if argument.startswith('--'):
                if '=' in argument:
                    prefix, default = argument.split('=', 1)
                    prefix += '='
                    default, type, itemSeparator = self.guess_type(
                        default, ignore_filenames=output_streams
                    )
                    # TODO can be output

                    position += 1
                    yield CommandInputParameter(
                        id='input_{0}'.format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position,
                            itemSeparator=itemSeparator,
                            prefix=prefix,
                            separate=False,
                        )
                    )
                    prefix = None
                else:
                    prefix = argument

            elif argument.startswith('-'):
                if len(argument) > 2:
                    if '=' in argument:
                        prefix, default = argument.split('=', 1)
                        prefix += '='
                        default, type, itemSeparator = self.guess_type(
                            default, ignore_filenames=output_streams
                        )
                    else:
                        # possibly a flag with value
                        prefix = argument[0:2]
                        default, type, itemSeparator = self.guess_type(
                            argument[2:], ignore_filenames=output_streams
                        )

                    position += 1
                    yield CommandInputParameter(
                        id='input_{0}'.format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position,
                            itemSeparator=itemSeparator,
                            prefix=prefix,
                            separate=not bool(argument[2:]),
                        )
                    )
                    prefix = None
                else:
                    prefix = argument

            else:
                default, type, itemSeparator = self.guess_type(
                    argument, ignore_filenames=output_streams
                )
                # TODO can be output

                # TODO there might be an array
                position += 1
                yield CommandInputParameter(
                    id='input_{0}'.format(position),
                    type=type,
                    default=default,
                    inputBinding=dict(
                        position=position,
                        itemSeparator=itemSeparator,
                        prefix=prefix,
                    )
                )
                prefix = None

        if prefix:
            position += 1
            yield CommandLineBinding(
                position=position,
                valueFrom=prefix,
            )

    def guess_outputs(self, paths):
        """Yield detected output and changed command input parameter."""
        # TODO what to do with duplicate paths & inputs with same defauts
        paths = list(paths)
        tree = DirectoryTree.from_list(paths)

        input_candidates = {}
        conflicting_paths = {}

        for index, input in enumerate(self.inputs):
            # Convert input defaults to paths relative to working directory.
            if input.type not in PATH_OBJECTS:
                try:
                    path = self.directory / str(input.default)
                    input_path = path.resolve().relative_to(self.working_dir)
                except FileNotFoundError:
                    continue
            else:
                input_path = input.default.path.relative_to(self.working_dir)

            if input_path.is_dir() and tree.get(input_path):
                # The directory might exist before running the script
                subpaths = {
                    str(input_path / path)
                    for path in tree.get(input_path, default=[])
                }
                absolute_path = os.path.abspath(input_path)
                if Path(absolute_path) not in self.explicit_outputs:
                    content = {
                        str(path)
                        for path in input_path.rglob('*')
                        if not path.is_dir() and path.name != '.gitkeep'
                    }
                    extra_paths = content - subpaths
                    if extra_paths:
                        raise errors.InvalidOutputPath(
                            'The output directory "{0}" is not empty. \n\n'
                            'Delete existing files before running the '
                            'command:'
                            '\n  (use "git rm <file>..." to remove them '
                            'first)'
                            '\n\n'.format(input_path) + '\n'.join(
                                '\t' + click.style(path, fg='yellow')
                                for path in extra_paths
                            ) + '\n\n'
                            'Once you have removed files that should be used '
                            'as outputs,\n'
                            'you can safely rerun the previous command.'
                        )

                # Remove files from the input directory
                paths = [path for path in paths if path not in subpaths]
                # Include input path in the paths to check
                paths.append(str(input_path))

                input_candidates[str(input_path)] = input
            elif input.type not in PATH_OBJECTS:
                # Input need to be changed if an output is detected
                input_candidates[str(input_path)] = input
            else:
                # Names that can not be outputs because they are already inputs
                conflicting_paths[str(input_path)] = (index, input)

        streams = {
            path
            for path in (getattr(self, name) for name in ('stdout', 'stderr'))
            if path is not None
        }

        # TODO group by a common prefix

        for position, path in enumerate(paths):
            if Path(os.path.abspath(path)) in self.explicit_outputs:
                del paths[position]

        for position, path in enumerate(paths):
            candidate = self.file_candidate(self.working_dir / path)

            if candidate is None:
                raise ValueError('Path "{0}" does not exist.'.format(path))

            glob = str(candidate.relative_to(self.working_dir))

            if glob in streams:
                continue

            new_input = None

            if glob in conflicting_paths:
                # it means that it is rewriting a file
                index, input = conflicting_paths[glob]
                new_input = attr.evolve(input, type='string', default=glob)
                input_candidates[glob] = new_input

                del conflicting_paths[glob]
                # TODO add warning ('Output already exists in inputs.')

            candidate_type = 'Directory' if candidate.is_dir() else 'File'

            if glob in input_candidates:
                input = input_candidates[glob]

                if new_input is None:
                    new_input = input_candidates[glob] = attr.evolve(
                        input, type='string', default=glob
                    )

                yield (
                    CommandOutputParameter(
                        id='output_{0}'.format(position),
                        type=candidate_type,
                        outputBinding=dict(
                            glob='$(inputs.{0})'.format(input.id),
                        ),
                    ), new_input, glob
                )
            else:
                yield (
                    CommandOutputParameter(
                        id='output_{0}'.format(position),
                        type=candidate_type,
                        outputBinding=dict(glob=glob, ),
                    ), None, glob
                )

    def find_explicit_inputs(self):
        """Yield explicit inputs and command line input bindings if any."""
        input_paths = [
            input.default.path
            for input in self.inputs if input.type in PATH_OBJECTS
        ]
        input_id = len(self.inputs) + len(self.arguments)

        for explicit_input in self.explicit_inputs:
            if explicit_input in input_paths:
                continue

            try:
                explicit_input.relative_to(self.working_dir)
            except ValueError:
                raise errors.InvalidInputPath(
                    'The input file or directory is not in the repository.'
                    '\n\n\t' + click.style(str(explicit_input), fg='yellow') +
                    '\n\n'
                )
            if self.file_candidate(explicit_input) is None:
                raise errors.InvalidInputPath(
                    'The input file or directory does not exist.'
                    '\n\n\t' + click.style(str(explicit_input), fg='yellow') +
                    '\n\n'
                )
            input_id += 1
            default, type, _ = self.guess_type(explicit_input)
            # Explicit inputs are either File or Directory
            assert type in PATH_OBJECTS
            # The inputBinging is None because these inputs won't
            # appear on command-line
            yield CommandInputParameter(
                id='input_{0}'.format(input_id),
                type=type,
                default=default,
                inputBinding=None
            )

    def find_explicit_outputs(self, starting_output_id):
        """Yield explicit output and changed command input parameter."""
        inputs = {
            str(i.default.path.relative_to(self.working_dir)): i
            for i in self.inputs if i.type in PATH_OBJECTS
        }
        output_id = starting_output_id

        for path in self.explicit_outputs:
            if self.file_candidate(path) is None:
                raise errors.InvalidOutputPath(
                    'The output file or directory does not exist.'
                    '\n\n\t' + click.style(str(path), fg='yellow') + '\n\n'
                )

            output_path = str(path.relative_to(self.working_dir))
            type = 'Directory' if path.is_dir() else 'File'
            if output_path in inputs:
                # change input type to note that it is also an output
                input = inputs[output_path]
                input = attr.evolve(input, type='string', default=output_path)
                yield (
                    CommandOutputParameter(
                        id='output_{0}'.format(output_id),
                        type=type,
                        outputBinding=dict(
                            glob='$(inputs.{0})'.format(input.id)
                        )
                    ), input, output_path
                )
            else:
                yield (
                    CommandOutputParameter(
                        id='output_{0}'.format(output_id),
                        type=type,
                        outputBinding=dict(glob=str(output_path))
                    ), None, output_path
                )

            output_id += 1

    def delete_indirect_files_list(self):
        """Remove indirect inputs and outputs list."""
        try:
            path = str(self.working_dir / INDIRECT_INPUTS_LIST)
            os.remove(path)
        except FileNotFoundError:
            pass
        try:
            path = str(self.working_dir / INDIRECT_OUTPUTS_LIST)
            os.remove(path)
        except FileNotFoundError:
            pass

    def add_indirect_inputs(self):
        """Read indirect inputs list and add them to explicit inputs."""
        for indirect_input in self.read_files_list(INDIRECT_INPUTS_LIST):
            # treat indirect inputs like explicit inputs
            self.explicit_inputs.append(indirect_input)

        # add new explicit inputs (if any) to inputs
        for input in self.find_explicit_inputs():
            self.inputs.append(input)

    def add_indirect_outputs(self):
        """Read indirect outputs list and add them to explicit outputs."""
        for indirect_output in self.read_files_list(INDIRECT_OUTPUTS_LIST):
            # treat indirect outputs like explicit outputs
            self.explicit_outputs.append(indirect_output)

    def read_files_list(self, files_list):
        """Read files list where each line is a filepath."""
        try:
            path = str(files_list)
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield Path(os.path.abspath(line))
        except FileNotFoundError:
            return
