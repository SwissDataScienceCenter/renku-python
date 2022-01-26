# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Represent a ``PlanFactory`` for tracking workflows."""

import os
import re
import shlex
import time
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple, Union

import click
import yaml

from renku.core import errors
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.workflow.types import PATH_OBJECTS, Directory, File
from renku.core.models.datastructures import DirectoryTree
from renku.core.models.workflow.parameter import (
    DIRECTORY_MIME_TYPE,
    CommandInput,
    CommandOutput,
    CommandParameter,
    MappedIOStream,
)
from renku.core.models.workflow.plan import Plan
from renku.core.utils.git import is_path_safe
from renku.core.utils.metadata import is_external_file
from renku.core.utils.os import get_relative_path

STARTED_AT = int(time.time() * 1000)

RENKU_TMP = "tmp"


class PlanFactory:
    """Factory for creating a plan from a command line call."""

    _RE_SUBCOMMAND = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)?$")

    def __init__(
        self,
        command_line: str,
        explicit_inputs: Optional[List[Tuple[str, Optional[str]]]] = None,
        explicit_outputs: Optional[List[Tuple[str, Optional[str]]]] = None,
        explicit_parameters: Optional[List[Tuple[str, Optional[str]]]] = None,
        directory: Optional[str] = None,
        working_dir: Optional[str] = None,
        no_input_detection: bool = False,
        no_output_detection: bool = False,
        success_codes: List[int] = None,
        stdin: Optional[str] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ):
        self.plan_id = Plan.generate_id()

        self.no_input_detection = no_input_detection
        self.no_output_detection = no_output_detection

        if not command_line:
            raise errors.UsageError("Command line can not be empty.")

        if not directory:
            directory = os.getcwd()
        self.directory = Path(directory)
        if not self.directory.exists():
            raise errors.UsageError("Directory must exist.")

        if not working_dir:
            working_dir = os.getcwd()
        self.working_dir = Path(working_dir)
        if not self.working_dir.exists():
            raise errors.UsageError("Working Directory must exist.")

        if isinstance(command_line, (list, tuple)):
            self.command_line = list(command_line)
        else:
            self.command_line = shlex.split(command_line)

        self.success_codes = success_codes or []

        self.explicit_inputs = (
            [(Path(os.path.abspath(path)), name) for path, name in explicit_inputs] if explicit_inputs else []
        )
        self.explicit_outputs = (
            [(Path(os.path.abspath(path)), name) for path, name in explicit_outputs] if explicit_outputs else []
        )
        self.explicit_parameters = explicit_parameters if explicit_parameters else []

        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        self.base_command, detected_arguments = self.split_command_and_args()
        self.parameters = []
        self.inputs = []
        self.outputs = []
        self.annotations = []
        self.existing_directories = set()

        self.add_inputs_and_parameters(*detected_arguments)

        self.existing_directories = {}

    def split_command_and_args(self):
        """Return tuple with command and args from command line arguments."""
        existing_subpath = self._resolve_existing_subpath(self.command_line[0])
        if existing_subpath is not None:
            return [], list(self.command_line)

        cmd = [self.command_line[0]]
        args = list(self.command_line[1:])

        if len(args) < 2:
            # only guess subcommand for more arguments
            return cmd, args

        while args and re.match(self._RE_SUBCOMMAND, args[0]) and not self._resolve_existing_subpath(args[0]):
            cmd.append(args.pop(0))

        return cmd, args

    @staticmethod
    def _is_ignored_path(candidate: Union[Path, str], ignored_list: Set[str] = None) -> bool:
        """Return True if the path is in ignored list."""
        return ignored_list and str(candidate) in ignored_list

    def _resolve_existing_subpath(self, candidate) -> Optional[Path]:
        """Return a path instance if it exists in the project's directory."""
        candidate = Path(candidate)

        if not candidate.is_absolute():
            candidate = self.directory / candidate

        if candidate.exists() or candidate.is_symlink():
            path = candidate.resolve()

            # NOTE: If relative_path is None then it's is either an external file or an absolute path (e.g. /bin/bash)
            relative_path = get_relative_path(path=path, base=self.working_dir)
            if relative_path is not None:
                return path
            elif is_external_file(path=candidate, client_path=self.working_dir):
                return Path(os.path.abspath(candidate))

    def add_inputs_and_parameters(self, *arguments):
        """Yield command input parameters."""
        position = 0
        prefix = None

        output_streams = {getattr(self, stream_name) for stream_name in ("stdout", "stderr")}

        for index, argument in enumerate(arguments):

            if prefix:
                if argument.startswith("-"):
                    position += 1
                    self.add_command_parameter(default_value=prefix, position=position)
                    prefix = None

            if argument.startswith("--"):
                if "=" in argument:
                    prefix, default = argument.split("=", 1)
                    prefix += "="
                    default, type = self.guess_type(default, ignore_filenames=output_streams)

                    position += 1
                    if type in PATH_OBJECTS:
                        self.add_command_input(
                            default_value=self._path_relative_to_root(default.path),
                            prefix=prefix,
                            position=position,
                            encoding_format=[DIRECTORY_MIME_TYPE] if type == "Directory" else default.mime_type,
                        )
                    else:
                        self.add_command_parameter(default_value=default, prefix=prefix, position=position)

                    prefix = None
                else:
                    prefix = argument

            elif argument.startswith("-"):
                if len(argument) > 2:
                    if "=" in argument:
                        prefix, default = argument.split("=", 1)
                        prefix += "="
                        default, type = self.guess_type(default, ignore_filenames=output_streams)
                    else:
                        # possibly a flag with value
                        prefix = argument[0:2]
                        default, type = self.guess_type(argument[2:], ignore_filenames=output_streams)

                    position += 1

                    if type in PATH_OBJECTS:
                        self.add_command_input(
                            default_value=self._path_relative_to_root(default.path),
                            prefix=prefix,
                            position=position,
                            encoding_format=[DIRECTORY_MIME_TYPE] if type == "Directory" else default.mime_type,
                        )
                    else:
                        self.add_command_parameter(default_value=default, prefix=prefix, position=position)
                    prefix = None
                else:
                    prefix = argument

            else:
                default, type = self.guess_type(argument, ignore_filenames=output_streams)

                position += 1

                if prefix:
                    prefix = prefix + " "  # NOTE: Make sure prefix is separated from argument by space

                if type in PATH_OBJECTS:
                    self.add_command_input(
                        default_value=self._path_relative_to_root(default.path),
                        prefix=prefix,
                        position=position,
                        encoding_format=[DIRECTORY_MIME_TYPE] if type == "Directory" else default.mime_type,
                    )
                else:
                    self.add_command_parameter(default_value=default, prefix=prefix, position=position)
                prefix = None

        if prefix:
            position += 1
            self.add_command_parameter(default_value=prefix, position=position)

        if self.stdin:
            position += 1
            default, type = self.guess_type(str(self.working_dir / self.stdin), ignore_filenames=output_streams)
            assert isinstance(default, File)
            self.add_command_input(default_value=str(default), encoding_format=default.mime_type, position=position)

    def add_outputs(self, candidates: Set[Tuple[Union[Path, str], Optional[str]]]):
        """Yield detected output and changed command input parameter."""
        # TODO what to do with duplicate paths & inputs with same defaults
        candidate_paths = list(map(lambda x: x[0], candidates))
        tree = DirectoryTree.from_list(candidate_paths)

        input_candidates = {}
        parameter_candidates = {}

        for input in self.inputs:
            # NOTE: Check if an input directory was modified
            input_path = Path(input.default_value)

            if input_path.is_dir() and tree.get(input_path):
                # The directory might exist before running the script
                candidates = self._check_potential_output_directory(input_path, candidates, tree)

            input_candidates[str(input_path)] = input

        for parameter in self.parameters:
            # NOTE: find parameters that might actually be outputs
            if any(parameter.default_value == value for value, _ in self.explicit_parameters):
                continue

            try:
                path = self.directory / str(parameter.default_value)
                input_path = Path(os.path.abspath(path)).relative_to(self.working_dir)
            except FileNotFoundError:
                continue

            if input_path.is_dir() and tree.get(input_path):
                # The directory might exist before running the script
                candidates = self._check_potential_output_directory(input_path, candidates, tree)

                parameter_candidates[str(input_path)] = parameter
            parameter_candidates[str(input_path)] = parameter

        for path, name in candidates:
            candidate = self._resolve_existing_subpath(self.working_dir / path)

            if candidate is None:
                raise errors.UsageError('Path "{0}" does not exist.'.format(path))

            glob = str(candidate.relative_to(self.working_dir))

            if glob in input_candidates:
                input = input_candidates[glob]

                self.add_command_output_from_input(input, name=name)
            elif glob in parameter_candidates:
                param = parameter_candidates[glob]

                self.add_command_output_from_parameter(param, name=name)
            else:
                encoding_format = [DIRECTORY_MIME_TYPE] if candidate.is_dir() else self._get_mimetype(candidate)
                self.add_command_output(default_value=glob, encoding_format=encoding_format, name=name)

    def _check_potential_output_directory(
        self, input_path: Path, candidates: Set[Tuple[str, Optional[str]]], tree: DirectoryTree
    ) -> Set[Tuple[str, Optional[str]]]:
        """Check an input/parameter for being a potential output directory."""
        subpaths = {str(input_path / path) for path in tree.get(input_path, default=[])}
        absolute_path = os.path.abspath(input_path)
        if all(Path(absolute_path) != path for path, _ in self.explicit_outputs):
            content = {str(path) for path in input_path.rglob("*") if not path.is_dir() and path.name != ".gitkeep"}
            preexisting_paths = content - subpaths
            if preexisting_paths:
                raise errors.InvalidOutputPath(
                    'The output directory "{0}" is not empty. \n\n'
                    "Delete existing files before running the "
                    "command:"
                    '\n  (use "git rm <file>..." to remove them '
                    "first)"
                    "\n\n".format(input_path)
                    + "\n".join("\t" + click.style(path, fg="yellow") for path in preexisting_paths)
                    + "\n\n"
                    "Once you have removed files that should be used "
                    "as outputs,\n"
                    "you can safely rerun the previous command."
                )

        # Remove files from the input directory
        candidates = {(path, name) for path, name in candidates if path not in subpaths}
        # Include input path in the candidates to check
        candidates.add((str(input_path), None))

        return candidates

    @staticmethod
    def _get_mimetype(file: Path) -> List[str]:
        """Return the MIME-TYPE of the given file."""
        # TODO: specify the actual mime-type of the file
        return ["application/octet-stream"]

    def guess_type(self, value: Union[Path, str], ignore_filenames: Set[str] = None) -> Tuple[Any, str]:
        """Return new value and CWL parameter type."""
        if not self._is_ignored_path(value, ignore_filenames) and all(value != v for v, _ in self.explicit_parameters):
            candidate = self._resolve_existing_subpath(value)
            if candidate:
                if candidate.is_dir():
                    return Directory(path=candidate), "Directory"
                return File(path=candidate, mime_type=self._get_mimetype(candidate)), "File"

        return value, "string"

    def get_stream_mapping_for_value(self, value: Any):
        """Return a stream mapping if value is a path mapped to a stream."""
        if self.stdin and self.stdin == value:
            return MappedIOStream(id=MappedIOStream.generate_id("stdin"), stream_type="stdin")
        if self.stdout and self.stdout == value:
            return MappedIOStream(id=MappedIOStream.generate_id("stdout"), stream_type="stdout")
        if self.stderr and self.stderr == value:
            return MappedIOStream(id=MappedIOStream.generate_id("stderr"), stream_type="stderr")

    def add_command_input(
        self,
        default_value: Any,
        prefix: Optional[str] = None,
        position: Optional[int] = None,
        postfix: Optional[str] = None,
        name: Optional[str] = None,
        encoding_format: List[str] = None,
    ):
        """Create a CommandInput."""
        if self.no_input_detection and all(Path(default_value).resolve() != path for path, _ in self.explicit_inputs):
            return

        mapped_stream = self.get_stream_mapping_for_value(default_value)

        self.inputs.append(
            CommandInput(
                id=CommandInput.generate_id(
                    plan_id=self.plan_id,
                    position=position,
                    postfix=mapped_stream.stream_type if mapped_stream else postfix,
                ),
                default_value=default_value,
                prefix=prefix,
                position=position,
                mapped_to=mapped_stream,
                encoding_format=encoding_format,
                postfix=postfix,
                name=name,
            )
        )

    def add_command_output(
        self,
        default_value: Any,
        prefix: Optional[str] = None,
        position: Optional[int] = None,
        postfix: Optional[str] = None,
        encoding_format: List[str] = None,
        name: Optional[str] = None,
    ):
        """Create a CommandOutput."""
        if self.no_output_detection and all(Path(default_value).resolve() != path for path, _ in self.explicit_outputs):
            return

        create_folder = False
        path = Path(default_value)
        full_path = Path(self._path_relative_to_root(default_value)).resolve()

        if (full_path.is_dir() and str(path) in self.existing_directories) or (
            not full_path.is_dir() and str(path.parent) in self.existing_directories
        ):
            create_folder = True

        mapped_stream = self.get_stream_mapping_for_value(default_value)

        if mapped_stream and position is None:
            position = (
                max((p.position for p in chain(self.inputs, self.outputs, self.parameters) if p.position), default=0)
                + 1
            )

        self.outputs.append(
            CommandOutput(
                id=CommandOutput.generate_id(
                    plan_id=self.plan_id,
                    position=position,
                    postfix=mapped_stream.stream_type if mapped_stream else postfix,
                ),
                default_value=default_value,
                prefix=prefix,
                position=position,
                mapped_to=mapped_stream,
                encoding_format=encoding_format,
                postfix=postfix,
                create_folder=create_folder,
                name=name,
            )
        )

    def add_command_output_from_input(self, input: CommandInput, name):
        """Create a CommandOutput from an input."""
        self.inputs.remove(input)
        self.outputs.append(
            CommandOutput(
                id=input.id.replace("/inputs/", "/outputs/"),
                default_value=input.default_value,
                prefix=input.prefix,
                position=input.position,
                mapped_to=input.mapped_to,
                encoding_format=input.encoding_format,
                name=name,
            )
        )

    def add_command_output_from_parameter(self, parameter: CommandParameter, name):
        """Create a CommandOutput from a parameter."""
        self.parameters.remove(parameter)
        value = Path(self._path_relative_to_root(parameter.default_value))
        encoding_format = [DIRECTORY_MIME_TYPE] if value.resolve().is_dir() else self._get_mimetype(value)
        self.add_command_output(
            default_value=str(value),
            prefix=parameter.prefix,
            position=parameter.position,
            encoding_format=encoding_format,
            name=name,
        )

    def add_command_parameter(
        self,
        default_value: Any,
        prefix: Optional[str] = None,
        position: Optional[int] = None,
        name: Optional[str] = None,
    ):
        """Create a CommandParameter."""
        self.parameters.append(
            CommandParameter(
                id=CommandParameter.generate_id(plan_id=self.plan_id, position=position),
                default_value=default_value,
                prefix=prefix,
                position=position,
                name=name,
            )
        )

    def add_explicit_inputs(self):
        """Add explicit inputs ."""
        input_paths = [input.default_value for input in self.inputs]
        input_id = len(self.inputs) + len(self.parameters)

        for explicit_input, name in self.explicit_inputs:
            try:
                relative_explicit_input = str(explicit_input.relative_to(self.working_dir))
            except ValueError:
                raise errors.UsageError(
                    "The input file or directory is not in the repository."
                    "\n\n\t" + click.style(str(explicit_input), fg="yellow") + "\n\n"
                )

            if relative_explicit_input in input_paths:
                if name:
                    existing_inputs = [i for i in self.inputs if i.default_value == relative_explicit_input]

                    for existing_input in existing_inputs:
                        existing_input.name = name
                continue

            input_paths.append(explicit_input)

            if self._resolve_existing_subpath(explicit_input) is None:
                raise errors.UsageError(
                    "The input file or directory does not exist."
                    "\n\n\t" + click.style(str(explicit_input), fg="yellow") + "\n\n"
                )
            input_id += 1
            default, type = self.guess_type(explicit_input)
            # Explicit inputs are either File or Directory
            assert type in PATH_OBJECTS
            self.add_command_input(
                default_value=str(default),
                postfix=str(input_id),
                encoding_format=[DIRECTORY_MIME_TYPE] if type == "Directory" else default.mime_type,
                name=name,
            )

    def add_explicit_parameters(self):
        """Add explicit parameters."""
        parameter_names = [parameter.name for parameter in self.parameters]

        for explicit_parameter, name in self.explicit_parameters:
            if name and name in parameter_names:
                continue

            existing_parameters = [p for p in self.parameters if p.default_value == explicit_parameter]

            if existing_parameters and name:
                # NOTE: Update names of existing parameters
                for p in existing_parameters:
                    p.name = name
            elif not existing_parameters:
                self.add_command_parameter(explicit_parameter, name=name)

    @contextmanager
    @inject.autoparams()
    def watch(self, client_dispatcher: IClientDispatcher, no_output=False):
        """Watch a Renku repository for changes to detect outputs."""
        client = client_dispatcher.current_client
        client.check_external_storage()

        repository = client.repository

        # Remove indirect files list if any
        delete_indirect_files_list(self.working_dir)

        from renku.core.plugins.pluginmanager import get_plugin_manager

        pm = get_plugin_manager()
        pm.hook.pre_run(tool=self)
        self.existing_directories = {str(p.relative_to(client.path)) for p in client.path.glob("**/")}

        yield self

        if repository:
            # Include indirect inputs and outputs before further processing
            self.add_indirect_inputs()
            self.add_indirect_outputs()

            self._include_indirect_parameters()

            # Remove indirect files list if any
            delete_indirect_files_list(self.working_dir)

            # List of all output paths.
            output_paths = []

            inputs = {input.id: input for input in self.inputs}

            # Keep track of unmodified output files.
            unmodified = set()

            candidates = set()

            if not self.no_output_detection:
                # Calculate possible output paths.
                # Capture newly created files through redirects.
                candidates |= {(file_, None) for file_ in repository.untracked_files}

                # Capture modified files through redirects.
                candidates |= {(o.b_path, None) for o in repository.unstaged_changes if not o.deleted}

            # Include explicit outputs
            candidates |= {(str(path.relative_to(self.working_dir)), name) for path, name in self.explicit_outputs}

            candidates = {(path, name) for path, name in candidates if is_path_safe(path)}

            self.add_outputs(candidates)

            for stream_name in ("stdout", "stderr"):
                stream = getattr(self, stream_name)
                if (
                    stream
                    and all(stream != path for path, _ in candidates)
                    and (Path(os.path.abspath(stream)) != path for path, _ in self.explicit_outputs)
                ):
                    unmodified.add(stream)
                elif stream:
                    output_paths.append(stream)

            for output in self.outputs:
                if output.default_value not in output_paths:
                    output_paths.append(output.default_value)

            if unmodified:
                raise errors.UnmodifiedOutputs(repository, unmodified)

            if not no_output and not output_paths:
                raise errors.OutputsNotFound(repository, inputs.values())

            if client.check_external_storage():
                client.track_paths_in_storage(*output_paths)

            client.repository.add(*output_paths)

        results = pm.hook.cmdline_tool_annotations(tool=self)
        self.annotations = [a for r in results for a in r]

    def _path_relative_to_root(self, path) -> str:
        """Make a potentially relative path in a subdirectory relative to the root of the repository."""
        return str((self.directory / path).resolve().relative_to(self.working_dir))

    def _include_indirect_parameters(self):
        run_parameters = read_indirect_parameters(self.working_dir)

        for k, v in run_parameters.items():
            self.explicit_parameters.append((str(v), k))

        self.add_explicit_parameters()

    def add_indirect_inputs(self):
        """Read indirect inputs list and add them to explicit inputs."""
        indirect_inputs_list = get_indirect_inputs_path(self.working_dir)

        for indirect_input in self._read_files_list(indirect_inputs_list):
            # treat indirect inputs like explicit inputs
            path = Path(os.path.abspath(indirect_input))
            self.explicit_inputs.append((path, None))

        # add new explicit inputs (if any) to inputs
        self.add_explicit_inputs()

    def add_indirect_outputs(self):
        """Read indirect outputs list and add them to explicit outputs."""
        indirect_outputs_list = get_indirect_outputs_path(self.working_dir)

        for indirect_output in self._read_files_list(indirect_outputs_list):
            # treat indirect outputs like explicit outputs
            path = Path(os.path.abspath(indirect_output))
            self.explicit_outputs.append((path, None))

    def iter_input_files(self, basedir):
        """Yield tuples with input id and path."""
        for input_ in self.inputs:
            yield input_.id, os.path.normpath(os.path.join(basedir, input_.default_value))

    @staticmethod
    def _read_files_list(files_list):
        """Read files list where each line is a filepath."""
        try:
            path = str(files_list)
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield Path(os.path.abspath(line))
        except FileNotFoundError:
            return

    @inject.autoparams("project_gateway")
    def to_plan(
        self,
        project_gateway: IProjectGateway,
        name: Optional[str] = None,
        description: str = Optional[None],
        keywords: Optional[List[str]] = None,
    ) -> Plan:
        """Return an instance of ``Plan`` based on this factory."""
        return Plan(
            id=self.plan_id,
            name=name,
            description=description,
            keywords=keywords,
            command=" ".join(self.base_command),
            inputs=self.inputs,
            outputs=self.outputs,
            parameters=self.parameters,
            project_id=project_gateway.get_project().id,
            success_codes=self.success_codes,
        )


def delete_indirect_files_list(working_dir):
    """Remove indirect inputs, outputs, and parameters list."""
    paths = [
        get_indirect_inputs_path(working_dir),
        get_indirect_outputs_path(working_dir),
        get_indirect_parameters_path(working_dir),
    ]
    for path in paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def get_indirect_inputs_path(client_path):
    """Return path to file that contains indirect inputs list."""
    parent = _get_indirect_parent_path(client_path)
    return parent / "inputs.txt"


def get_indirect_outputs_path(client_path):
    """Return path to file that contains indirect outputs list."""
    parent = _get_indirect_parent_path(client_path)
    return parent / "outputs.txt"


def get_indirect_parameters_path(client_path):
    """Return path to file that contains indirect parameters list."""
    parent = _get_indirect_parent_path(client_path)
    return parent / "parameters.yml"


def _get_indirect_parent_path(client_path):
    renku_indirect_path = os.getenv("RENKU_INDIRECT_PATH") or ""

    base = (Path(client_path) / RENKU_HOME / RENKU_TMP).resolve()
    parent = (base / renku_indirect_path).resolve()

    try:
        parent.relative_to(base)
    except ValueError:
        raise errors.InvalidFileOperation(f"Invalid value for RENKU_INDIRECT_PATH env var: {renku_indirect_path}.")

    return parent


def read_indirect_parameters(working_dir):
    """Read and return indirect parameters."""
    path = get_indirect_parameters_path(working_dir)

    if not path.exists():
        return {}

    data = yaml.safe_load(path.read_text())

    if not isinstance(data, dict):
        raise errors.OperationError("Run parameters must be a dictionary.")

    return data


def add_indirect_parameter(working_dir, name, value):
    """Add a parameter to indirect parameters."""
    data = read_indirect_parameters(working_dir)
    data[name] = value

    yaml_data = yaml.dump(data)

    path = get_indirect_parameters_path(working_dir)
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(yaml_data)
