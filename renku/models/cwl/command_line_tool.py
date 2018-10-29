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
"""Represent a ``CommandLineTool`` from the Common Workflow Language."""

import fnmatch
import re
import shlex
from contextlib import contextmanager

import attr
import click

from renku import errors
from renku._compat import Path

from .._datastructures import DirectoryTree
from ._ascwl import CWLClass, mapped
from .parameter import CommandInputParameter, CommandLineBinding, \
    CommandOutputParameter
from .process import Process
from .types import PATH_OBJECTS, Directory, File


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

    def __str__(self):
        """Generate a simple representation."""
        argv = ' '.join(self.to_argv())
        if self.stdin:
            assert self.stdin.startswith('$(inputs.')
            input_id = self.stdin.split('.')[1]
            for input_ in self.inputs:
                if input_id == input_.id:
                    argv += ' < ' + str(input_.default)
                    break
        if self.stdout:
            argv += ' > ' + self.stdout
        if self.stderr:
            argv += ' 2> ' + self.stderr
        return argv

    def create_run(self, **kwargs):
        """Return an instance of process run."""
        from renku.models.provenance import ProcessRun
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

        for p, v in sorted(args):
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

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.baseCommand, detect = self.split_command_and_args()
        self.arguments = []
        self.inputs = []
        self.outputs = []

        if self.stdin:
            input_ = next(self.guess_inputs(self.stdin))
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
    def watch(self, repo=None, no_output=False):
        """Watch a Renku repository for changes to detect outputs."""
        tool = self.generate_tool()
        git = repo.git
        # NOTE consider to use git index instead
        existing_directories = {
            str(p.relative_to(repo.path))
            for p in repo.path.glob('**/')
        }

        yield tool

        if git:
            # List of all output paths.
            paths = []
            # Keep track of unmodified output files.
            unmodified = set()
            # Possible output paths.
            candidates = set(git.untracked_files)
            candidates |= {
                item.a_path
                for item in git.index.diff(None) if not item.deleted_file
            }

            from renku.cli._graph import _safe_path
            candidates = {path for path in candidates if _safe_path(path)}

            inputs = {input.id: input for input in self.inputs}
            outputs = list(tool.outputs)

            for output, input, path in self.guess_outputs(candidates):
                outputs.append(output)
                paths.append(path)

                if input is not None:
                    if input.id not in inputs:  # pragma: no cover
                        raise RuntimeError('Inconsistent input name.')

                    inputs[input.id] = input

            for stream_name in ('stdout', 'stderr'):
                stream = getattr(self, stream_name)
                if stream and stream not in candidates:
                    unmodified.add(stream)
                elif stream:
                    paths.append(stream)

            if unmodified:
                raise errors.UnmodifiedOutputs(repo, unmodified)

            if not no_output and not paths:
                raise errors.OutputsNotFound(repo, inputs.values())

            tool.inputs = list(inputs.values())
            tool.outputs = outputs

            repo.track_paths_in_storage(*paths)

        # Requirement detection can be done anytime.
        from .process_requirements import InitialWorkDirRequirement, \
            InlineJavascriptRequirement
        initial_work_dir_requirement = InitialWorkDirRequirement.from_tool(
            tool, existing_directories=existing_directories
        )
        if initial_work_dir_requirement:
            tool.requirements.extend([
                InlineJavascriptRequirement(),
                initial_work_dir_requirement,
            ])

    @command_line.validator
    def validate_command_line(self, attribute, value):
        """Check the command line structure."""
        if not value:
            raise ValueError('Command line can not be empty.')

    @directory.validator
    def validate_path(self, attribute, value):
        """Path must exists."""
        if not value.exists():
            raise ValueError('Directory must exist.')

    def file_candidate(self, candidate, ignore=None):
        """Return a path instance if it exists in current directory."""
        if ignore and candidate in ignore:
            return

        candidate = Path(candidate)

        if not candidate.is_absolute():
            candidate = self.directory / candidate

        if candidate.exists():
            return candidate.resolve()

    def split_command_and_args(self):
        """Return tuple with command and args from command line arguments."""
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
                    input_path = (self.directory /
                                  str(input.default)).resolve().relative_to(
                                      self.working_dir
                                  )
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
                content = {
                    str(path)
                    for path in input_path.rglob('*')
                    if not path.is_dir() and path.name != '.gitkeep'
                }
                extra_paths = content - subpaths
                if extra_paths:
                    raise errors.InvalidOutputPath(
                        'The output directory "{0}" is not empty. \n\n'
                        'Delete existing files before running the command:'
                        '\n  (use "git rm <file>..." to remove them first)'
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
