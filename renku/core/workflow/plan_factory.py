# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union, cast

import click
import yaml

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.constant import RENKU_HOME, RENKU_TMP
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.plugin.pluginmanager import get_plugin_manager
from renku.core.storage import check_external_storage, track_paths_in_storage
from renku.core.util.git import is_path_safe
from renku.core.util.metadata import is_external_file
from renku.core.util.os import get_absolute_path, get_relative_path, is_subpath
from renku.core.workflow.types import PATH_OBJECTS, Directory, File
from renku.domain_model.datastructures import DirectoryTree
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person
from renku.domain_model.workflow.parameter import (
    DIRECTORY_MIME_TYPE,
    CommandInput,
    CommandOutput,
    CommandParameter,
    MappedIOStream,
)
from renku.domain_model.workflow.plan import Plan

STARTED_AT = int(time.time() * 1000)


class PlanFactory:
    """Factory for creating a plan from a command line call."""

    _RE_SUBCOMMAND = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)?$")

    command_line: List[str]

    def __init__(
        self,
        command_line: Union[str, List[str], Tuple[str, ...]],
        explicit_inputs: Optional[List[Tuple[str, str]]] = None,
        explicit_outputs: Optional[List[Tuple[str, str]]] = None,
        explicit_parameters: Optional[List[Tuple[str, Optional[str]]]] = None,
        directory: Optional[Union[Path, str]] = None,
        working_dir: Optional[Union[Path, str]] = None,
        no_input_detection: bool = False,
        no_output_detection: bool = False,
        success_codes: Optional[List[int]] = None,
        stdin: Optional[str] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ):
        self.plan_id = Plan.generate_id()

        self.no_input_detection = no_input_detection
        self.no_output_detection = no_output_detection

        if not command_line:
            raise errors.UsageError("Command line can not be empty. Please specify a command to execute.")

        if not directory:
            directory = os.getcwd()
        self.directory = Path(directory)
        if not self.directory.exists():
            raise errors.UsageError(
                f"Directory '{self.directory}' doesn't exist. Please make sure you are inside an existing directory."
            )

        if not working_dir:
            working_dir = os.getcwd()
        self.working_dir = Path(working_dir)
        if not self.working_dir.exists():
            raise errors.UsageError(
                f"Repository path '{self.working_dir}' doesn't exist. Make sure you are inside a Renku repository."
            )

        if isinstance(command_line, (list, tuple)):
            self.command_line = list(command_line)
        else:
            self.command_line = shlex.split(command_line)

        self.success_codes = success_codes or []

        self.explicit_inputs: List[Tuple[str, str]] = (
            [(get_absolute_path(path), name) for path, name in explicit_inputs] if explicit_inputs else []
        )
        self.explicit_outputs: List[Tuple[str, str]] = (
            [(get_absolute_path(path), name) for path, name in explicit_outputs] if explicit_outputs else []
        )
        self.explicit_parameters = explicit_parameters if explicit_parameters else []

        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        self.base_command, detected_arguments = self.split_command_and_args()
        self.parameters: List[CommandParameter] = []
        self.inputs: List[CommandInput] = []
        self.outputs: List[CommandOutput] = []
        self.annotations: List[Dict[str, Any]] = []
        self.existing_directories: Set[str] = set()

        self.add_inputs_and_parameters(*detected_arguments)

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
        return ignored_list is not None and str(candidate) in ignored_list

    def _resolve_existing_subpath(self, candidate: Union[Path, str]) -> Optional[Path]:
        """Return a path instance if it exists in the project's directory."""
        candidate = self.directory / candidate if not os.path.isabs(candidate) else Path(candidate)

        if candidate.exists() or candidate.is_symlink():
            path = candidate.resolve()

            # NOTE: If resolved path is not within the project then it's is either an external file or an absolute path
            # (e.g. /bin/bash)
            if is_subpath(path, base=self.working_dir):
                return path
            elif is_external_file(path=candidate, project_path=self.working_dir):
                return Path(os.path.abspath(candidate))

        return None

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

    def add_outputs(self, candidates: Iterable[Tuple[Union[Path, str], Optional[str]]]):
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
                candidates = self._check_potential_output_directory(input_path, candidates, tree)  # type: ignore

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
            except ValueError:
                # NOTE: Raised if path is not relative to working_dir (external file)
                input_path = Path(parameter.default_value)

                if not input_path.exists():
                    continue

            if input_path.is_dir() and tree.get(input_path):
                # The directory might exist before running the script
                candidates = self._check_potential_output_directory(input_path, candidates, tree)  # type: ignore

                parameter_candidates[str(input_path)] = parameter
            parameter_candidates[str(input_path)] = parameter

        for candidate_path, name in candidates:
            candidate = self._resolve_existing_subpath(self.working_dir / candidate_path)

            if candidate is None:
                raise errors.UsageError('Path "{0}" does not exist inside the current project.'.format(candidate_path))

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
        if not self._is_explicit(input_path, self.explicit_outputs):
            content = {str(path) for path in input_path.rglob("*") if not path.is_dir() and path.name != ".gitkeep"}
            preexisting_paths = content - subpaths
            if preexisting_paths:
                raise errors.InvalidOutputPath(
                    f"The output directory '{input_path}' is not empty. \n\n"
                    "As renku treats whole directory outputs as generated by renku,"
                    "those directories have to be empty before being tracked by renku.\n\n"
                    "You can solve this by:\n"
                    f"- Deleting the existing files in the directory (use 'git rm -r {input_path}')\n"
                    "- Using a different output folder\n"
                    "- Using a new empty subfolder inside the output folder\n\n"
                    "Output directories with existing files:\n\n"
                    + "\n".join("\t" + click.style(path, fg="yellow") for path in preexisting_paths)
                    + "\n\n"
                    "Once you have resolved the issues above, you can safely rerun the previous command."
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
        encoding_format: Optional[List[str]] = None,
    ):
        """Create a CommandInput."""
        if self.no_input_detection and not self._is_explicit(default_value, self.explicit_inputs):
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
        id: str = None,
        mapped_to: Optional[MappedIOStream] = None,
    ):
        """Create a CommandOutput."""
        if self.no_output_detection and not self._is_explicit(default_value, self.explicit_outputs):
            return

        create_folder = False
        path = Path(default_value)
        full_path = Path(self._path_relative_to_root(default_value)).resolve()

        if (full_path.is_dir() and str(path) in self.existing_directories) or (
            not full_path.is_dir() and str(path.parent) in self.existing_directories
        ):
            create_folder = True

        mapped_stream = mapped_to or self.get_stream_mapping_for_value(default_value)

        if mapped_stream and position is None:
            position = (
                max((p.position for p in chain(self.inputs, self.outputs, self.parameters) if p.position), default=0)
                + 1
            )

        id = id or CommandOutput.generate_id(
            plan_id=self.plan_id,
            position=position,
            postfix=mapped_stream.stream_type if mapped_stream else postfix,
        )

        self.outputs.append(
            CommandOutput(
                id=id,
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

        self.add_command_output(
            id=input.id.replace("/inputs/", "/outputs/"),
            default_value=input.default_value,
            prefix=input.prefix,
            position=input.position,
            mapped_to=input.mapped_to,
            encoding_format=input.encoding_format,
            name=name,
        )

    def add_command_output_from_parameter(self, parameter: CommandParameter, name):
        """Create a CommandOutput from a parameter."""
        self.parameters.remove(parameter)
        value = Path(self._path_relative_to_root(parameter.default_value))
        encoding_format = [DIRECTORY_MIME_TYPE] if value.is_dir() else self._get_mimetype(value)
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
                relative_explicit_input = get_relative_path(explicit_input, base=self.working_dir, strict=True)
            except errors.ParameterError:
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
    def watch(self, no_output=False):
        """Watch a Renku repository for changes to detect outputs."""
        check_external_storage()

        repository = project_context.repository

        # Remove indirect files list if any
        delete_indirect_files_list(self.working_dir)

        from renku.core.plugin.pluginmanager import get_plugin_manager

        pm = get_plugin_manager()
        pm.hook.pre_run(tool=self)
        self.existing_directories = {str(p.relative_to(project_context.path)) for p in project_context.path.glob("**/")}

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

            # Keep track of unmodified output files.
            unmodified = set()

            candidates: Set[Tuple[Union[Path, str], Optional[str]]] = set()

            if not self.no_output_detection:
                # Calculate possible output paths.
                # Capture newly created files through redirects.
                candidates |= {(file_, None) for file_ in repository.untracked_files}

                # Capture modified files through redirects.
                candidates |= {(o.b_path, None) for o in repository.unstaged_changes if not o.deleted}

                # Filter out explicit outputs
                explicit_output_paths = {
                    str(Path(path).relative_to(self.working_dir)) for path, _ in self.explicit_outputs
                }
                candidates = {c for c in candidates if c[0] not in explicit_output_paths}

            # Include explicit outputs
            candidates |= {
                (str(Path(path).relative_to(self.working_dir)), name) for path, name in self.explicit_outputs
            }

            candidates = {(path, name) for path, name in candidates if is_path_safe(path)}

            self.add_outputs(candidates)

            for stream_name in ("stdout", "stderr"):
                stream = getattr(self, stream_name)
                if (
                    stream
                    and all(stream != path for path, _ in candidates)
                    and not self._is_explicit(stream, self.explicit_outputs)
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
                raise errors.OutputsNotFound()

            if check_external_storage():
                track_paths_in_storage(*output_paths)

            repository.add(*output_paths)

    def _path_relative_to_root(self, path) -> str:
        """Make a potentially relative path in a subdirectory relative to the root of the repository."""
        absolute_path = get_absolute_path(path, base=self.directory)
        return cast(str, get_relative_path(absolute_path, base=self.working_dir, strict=True))

    def _include_indirect_parameters(self):
        run_parameters = read_indirect_parameters(self.working_dir)

        for k, v in run_parameters.items():
            self.explicit_parameters.append((str(v), k))

        self.add_explicit_parameters()

    def add_indirect_inputs(self):
        """Read indirect inputs list and add them to explicit inputs."""
        indirect_inputs_list = get_indirect_inputs_path(self.working_dir)

        for name, indirect_input in read_files_list(indirect_inputs_list).items():
            # treat indirect inputs like explicit inputs
            path = get_absolute_path(indirect_input)
            self.explicit_inputs.append((path, name))

        # add new explicit inputs (if any) to inputs
        self.add_explicit_inputs()

    def add_indirect_outputs(self):
        """Read indirect outputs list and add them to explicit outputs."""
        indirect_outputs_list = get_indirect_outputs_path(self.working_dir)

        for name, indirect_output in read_files_list(indirect_outputs_list).items():
            # treat indirect outputs like explicit outputs
            path = get_absolute_path(indirect_output)
            self.explicit_outputs.append((path, name))

    def iter_input_files(self, basedir):
        """Yield tuples with input id and path."""
        for input_ in self.inputs:
            yield input_.id, os.path.normpath(os.path.join(basedir, input_.default_value))

    @staticmethod
    def _is_explicit(path: Union[Path, str], explicits_collection: List[Tuple[str, str]]) -> bool:
        absolute_path = get_absolute_path(path)
        return any(absolute_path == path for path, _ in explicits_collection)

    @inject.autoparams("project_gateway")
    def to_plan(
        self,
        project_gateway: IProjectGateway,
        name: Optional[str] = None,
        description: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        creators: Optional[List[Person]] = None,
        date_created: Optional[datetime] = None,
    ) -> Plan:
        """Return an instance of ``Plan`` based on this factory."""
        plan = Plan(
            id=self.plan_id,
            name=name,
            date_created=date_created,
            description=description,
            keywords=keywords,
            command=" ".join(self.base_command),
            inputs=self.inputs,
            outputs=self.outputs,
            parameters=self.parameters,
            project_id=project_gateway.get_project().id,
            success_codes=self.success_codes,
            creators=creators,
        )

        pm = get_plugin_manager()

        plugin_annotations = list(chain.from_iterable(pm.hook.plan_annotations(plan=plan)))

        if plugin_annotations:
            plan.annotations.extend(plugin_annotations)

        return plan


def read_files_list(files_list: Path):
    """Read files list yaml containing name:path pairs."""

    if not files_list.exists():
        return {}

    data = yaml.safe_load(files_list.read_text())

    if not isinstance(data, dict):
        raise errors.OperationError("Explicit Inputs/Outputs/Parameters files list must be a YAML dictionary.")

    return data


def add_to_files_list(file_list_path: Path, name: str, path: Union[str, Path]):
    """Add a parameter to indirect parameters."""
    data = read_files_list(file_list_path)

    if name in data and data[name] != str(path):
        raise errors.ParameterError(f"Duplicate input/output name found: {name}")

    data[name] = str(path)

    yaml_data = yaml.dump(data)

    file_list_path.parent.mkdir(exist_ok=True, parents=True)
    file_list_path.write_text(yaml_data)


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


def get_indirect_inputs_path(project_path):
    """Return path to file that contains indirect inputs list."""
    parent = _get_indirect_parent_path(project_path)
    return parent / "inputs.yml"


def get_indirect_outputs_path(project_path):
    """Return path to file that contains indirect outputs list."""
    parent = _get_indirect_parent_path(project_path)
    return parent / "outputs.yml"


def get_indirect_parameters_path(project_path):
    """Return path to file that contains indirect parameters list."""
    parent = _get_indirect_parent_path(project_path)
    return parent / "parameters.yml"


def _get_indirect_parent_path(project_path):
    renku_indirect_path = os.getenv("RENKU_INDIRECT_PATH") or ""

    base = (Path(project_path) / RENKU_HOME / RENKU_TMP).resolve()
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

    if name in data and data[name] != value:
        raise errors.ParameterError(f"Duplicate parameter names found: {name}")
    data[name] = value

    yaml_data = yaml.dump(data)

    path = get_indirect_parameters_path(working_dir)
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(yaml_data)
