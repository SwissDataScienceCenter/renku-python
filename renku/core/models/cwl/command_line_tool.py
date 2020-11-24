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
"""Represent a ``CommandLineToolFactory`` for tracking workflows."""

import os
import re
import shlex
import time
from contextlib import contextmanager
from pathlib import Path

import attr
import click
import yaml
from git import Actor

from renku.core import errors
from renku.core.commands.echo import INFO
from renku.core.utils.scm import git_unicode_unescape
from renku.version import __version__, version_url

from ...management.config import RENKU_HOME
from ..datastructures import DirectoryTree
from .parameter import CommandInputParameter, CommandLineBinding, CommandOutputParameter, RunParameter
from .types import PATH_OBJECTS, Directory, File

STARTED_AT = int(time.time() * 1000)

RENKU_TMP = "tmp"


@attr.s
class CommandLineToolFactory(object):
    """Command Line Tool Factory."""

    _RE_SUBCOMMAND = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)?$")

    command_line = attr.ib(converter=lambda cmd: list(cmd) if isinstance(cmd, (list, tuple)) else shlex.split(cmd),)

    explicit_inputs = attr.ib(factory=list, converter=lambda paths: [Path(os.path.abspath(p)) for p in paths])
    explicit_outputs = attr.ib(factory=list, converter=lambda paths: [Path(os.path.abspath(p)) for p in paths])

    no_input_detection = attr.ib(default=False)
    no_output_detection = attr.ib(default=False)

    directory = attr.ib(default=".", converter=lambda path: Path(path).resolve(),)
    working_dir = attr.ib(default=".", converter=lambda path: Path(path).resolve(),)

    stdin = attr.ib(default=None)  # null, str, Expression
    stderr = attr.ib(default=None)  # null, str, Expression
    stdout = attr.ib(default=None)  # null, str, Expression

    baseCommand = attr.ib(init=False)
    arguments = attr.ib(init=False)
    inputs = attr.ib(init=False)
    outputs = attr.ib(init=False)
    run_parameters = attr.ib(default=None, init=False)

    successCodes = attr.ib(default=attr.Factory(list))  # list(int)

    annotations = attr.ib(default=None)

    _had_changes = False
    existing_directories = set()

    messages = attr.ib(default=None)
    warnings = attr.ib(default=None)

    def __attrs_post_init__(self):
        """Derive basic information."""
        self.baseCommand, detect = self.split_command_and_args()
        self.arguments = []
        self.inputs = []
        self.outputs = []
        self.run_parameters = []

        if self.stdin:
            input_ = next(self.guess_inputs(str(self.working_dir / self.stdin)))
            assert input_.type == "File"
            input_ = attr.evolve(input_, id="input_stdin", inputBinding=None,)  # do not include in tool arguments
            self.inputs.append(input_)
            self.stdin = "$(inputs.{0}.path)".format(input_.id)

        for stream_name in ("stdout", "stderr"):
            stream = getattr(self, stream_name)
            if stream and self.is_existing_path(self.working_dir / stream):
                self.outputs.append(CommandOutputParameter(id="output_{0}".format(stream_name), type=stream_name,))

        for input_ in self.guess_inputs(*detect):
            if isinstance(input_, CommandLineBinding):
                self.arguments.append(input_)
            elif (
                not self.no_input_detection
                or input_.type not in PATH_OBJECTS
                or input_.default.path in self.explicit_inputs
            ):
                self.inputs.append(input_)

        if self.explicit_inputs:
            for input in self.find_explicit_inputs():
                self.inputs.append(input)

    def generate_process_run(self, client, commit, path):
        """Return an instance of ``ProcessRun``."""
        from ..provenance.activities import ProcessRun
        from ..workflow.run import Run

        run = Run.from_factory(factory=self, client=client, commit=commit, path=path,)

        process_run = ProcessRun.from_run(run, client, path, commit)

        if not self._had_changes:
            process_run.invalidated = []

        if hasattr(self, "annotations") and self.annotations:
            process_run.add_annotations(self.annotations)

        return process_run

    def iter_input_files(self, basedir):
        """Yield tuples with input id and path."""
        stdin = getattr(self, "stdin", None)
        if stdin and stdin[0] != "$":  # pragma: no cover
            raise NotImplementedError(self.stdin)
        for input_ in self.inputs:
            if input_.type in PATH_OBJECTS and input_.default:
                yield (input_.id, os.path.normpath(os.path.join(basedir, str(input_.default.path))))

    @contextmanager
    def watch(self, client, no_output=False):
        """Watch a Renku repository for changes to detect outputs."""
        client.check_external_storage()

        repo = client.repo

        # Remove indirect files list if any
        delete_indirect_files_list(self.working_dir)

        from renku.core.plugins.pluginmanager import get_plugin_manager

        pm = get_plugin_manager()
        pm.hook.pre_run(tool=self)
        self.existing_directories = {str(p.relative_to(client.path)) for p in client.path.glob("**/")}

        yield self

        if repo:
            # Include indirect inputs and outputs before further processing
            self.add_indirect_inputs()
            self.add_indirect_outputs()

            self._include_indirect_parameters()

            # Remove indirect files list if any
            delete_indirect_files_list(self.working_dir)

            # List of all output paths.
            output_paths = []

            inputs = {input.id: input for input in self.inputs}
            outputs = list(self.outputs)

            # Keep track of unmodified output files.
            unmodified = set()

            candidates = set()

            if not self.no_output_detection:
                # Calculate possible output paths.
                # Capture newly created files through redirects.
                candidates |= {file_ for file_ in repo.untracked_files}

                # Capture modified files through redirects.
                candidates |= {git_unicode_unescape(o.a_path) for o in repo.index.diff(None) if not o.deleted_file}

            # Include explicit outputs
            candidates |= {str(path.relative_to(self.working_dir)) for path in self.explicit_outputs}

            from renku.core.commands.graph import _safe_path

            candidates = {path for path in candidates if _safe_path(path)}

            for output, input, path in self.guess_outputs(candidates):
                outputs.append(output)
                output_paths.append(path)

                if input is not None:
                    if input.id not in inputs:  # pragma: no cover
                        raise RuntimeError("Inconsistent input name.")

                    inputs[input.id] = input

            for stream_name in ("stdout", "stderr"):
                stream = getattr(self, stream_name)
                if stream and stream not in candidates and Path(os.path.abspath(stream)) not in self.explicit_outputs:
                    unmodified.add(stream)
                elif stream:
                    output_paths.append(stream)

            if unmodified:
                raise errors.UnmodifiedOutputs(repo, unmodified)

            if not no_output and not output_paths:
                raise errors.OutputsNotFound(repo, inputs.values())

            if client.check_external_storage():
                lfs_paths = client.track_paths_in_storage(*output_paths)

                show_message = client.get_value("renku", "show_lfs_message")
                if lfs_paths and (show_message is None or show_message == "True"):
                    self.messages = (
                        INFO
                        + "Adding these files to Git LFS:\n"
                        + "\t{}".format("\n\t".join(lfs_paths))
                        + "\nTo disable this message in the future, run:"
                        + "\n\trenku config set show_lfs_message False"
                    )

            repo.git.add(*output_paths)

            if repo.is_dirty():
                commit_msg = f"renku run: committing {len(output_paths)} newly added files"

                committer = Actor("renku {0}".format(__version__), version_url)

                repo.index.commit(
                    commit_msg, committer=committer, skip_hooks=True,
                )

                self._had_changes = True

            self.inputs = list(inputs.values())
            self.outputs = outputs

        results = pm.hook.cmdline_tool_annotations(tool=self)
        self.annotations = [a for r in results for a in r]

    @command_line.validator
    def validate_command_line(self, attribute, value):
        """Check the command line structure."""
        if not value:
            raise errors.UsageError("Command line can not be empty.")

    @directory.validator
    def validate_path(self, attribute, value):
        """Path must exists."""
        if not value.exists():
            raise errors.UsageError("Directory must exist.")

    def is_existing_path(self, candidate, ignore=None):
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
        if self.is_existing_path(self.command_line[0]):
            return [], list(self.command_line)

        cmd = [self.command_line[0]]
        args = list(self.command_line[1:])

        if len(args) < 2:
            # only guess subcommand for more arguments
            return cmd, args

        while args and re.match(self._RE_SUBCOMMAND, args[0]) and not self.is_existing_path(args[0]):
            cmd.append(args.pop(0))

        return cmd, args

    def guess_type(self, value, ignore_filenames=None):
        """Return new value and CWL parameter type."""
        candidate = self.is_existing_path(value, ignore=ignore_filenames)
        if candidate:
            try:
                if candidate.is_dir():
                    return Directory(path=candidate), "Directory", None
                return File(path=candidate), "File", None
            except ValueError:
                # The candidate points to a file outside the working
                # directory
                # TODO suggest that the file should be imported to the repo
                pass

        try:
            value = int(value)
            return value, "int", None
        except ValueError:
            pass

        if len(value) > 1 and "," in value:
            return value.split(","), "string[]", ","

        return value, "string", None

    def guess_inputs(self, *arguments):
        """Yield command input parameters and command line bindings."""
        position = 0
        prefix = None

        output_streams = {getattr(self, stream_name) for stream_name in ("stdout", "stderr")}

        for index, argument in enumerate(arguments):
            itemSeparator = None

            if prefix:
                if argument.startswith("-"):
                    position += 1
                    yield CommandLineBinding(
                        position=position, valueFrom=prefix,
                    )
                    prefix = None

            if argument.startswith("--"):
                if "=" in argument:
                    prefix, default = argument.split("=", 1)
                    prefix += "="
                    default, type, itemSeparator = self.guess_type(default, ignore_filenames=output_streams)
                    # TODO can be output

                    position += 1
                    yield CommandInputParameter(
                        id="input_{0}".format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position, itemSeparator=itemSeparator, prefix=prefix, separate=False,
                        ),
                    )
                    prefix = None
                else:
                    prefix = argument

            elif argument.startswith("-"):
                if len(argument) > 2:
                    if "=" in argument:
                        prefix, default = argument.split("=", 1)
                        prefix += "="
                        default, type, itemSeparator = self.guess_type(default, ignore_filenames=output_streams)
                    else:
                        # possibly a flag with value
                        prefix = argument[0:2]
                        default, type, itemSeparator = self.guess_type(argument[2:], ignore_filenames=output_streams)

                    position += 1
                    yield CommandInputParameter(
                        id="input_{0}".format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position,
                            itemSeparator=itemSeparator,
                            prefix=prefix,
                            separate=not bool(argument[2:]),
                        ),
                    )
                    prefix = None
                else:
                    prefix = argument

            else:
                default, type, itemSeparator = self.guess_type(argument, ignore_filenames=output_streams)
                # TODO can be output

                # TODO there might be an array
                position += 1
                yield CommandInputParameter(
                    id="input_{0}".format(position),
                    type=type,
                    default=default,
                    inputBinding=dict(position=position, itemSeparator=itemSeparator, prefix=prefix,),
                )
                prefix = None

        if prefix:
            position += 1
            yield CommandLineBinding(
                position=position, valueFrom=prefix,
            )

    def guess_outputs(self, candidates):
        """Yield detected output and changed command input parameter."""
        # TODO what to do with duplicate paths & inputs with same defaults
        candidates = list(candidates)
        tree = DirectoryTree.from_list(candidates)

        input_candidates = {}
        conflicting_paths = {}

        for index, input in enumerate(self.inputs):
            # Convert input defaults to paths relative to working directory.
            if input.type not in PATH_OBJECTS:
                if self.no_input_detection:
                    continue
                try:
                    path = self.directory / str(input.default)
                    input_path = Path(os.path.abspath(path)).relative_to(self.working_dir)
                except FileNotFoundError:
                    continue
            else:
                input_path = input.default.path.relative_to(self.working_dir)

            if input_path.is_dir() and tree.get(input_path):
                # The directory might exist before running the script
                subpaths = {str(input_path / path) for path in tree.get(input_path, default=[])}
                absolute_path = os.path.abspath(input_path)
                if Path(absolute_path) not in self.explicit_outputs:
                    content = {
                        str(path) for path in input_path.rglob("*") if not path.is_dir() and path.name != ".gitkeep"
                    }
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
                candidates[:] = (path for path in candidates if path not in subpaths)
                # Include input path in the candidates to check
                candidates.append(str(input_path))

                input_candidates[str(input_path)] = input
            elif input.type not in PATH_OBJECTS:
                # Input need to be changed if an output is detected
                input_candidates[str(input_path)] = input
            else:
                # Names that can not be outputs because they are already inputs
                conflicting_paths[str(input_path)] = input

        streams = {path for path in (getattr(self, name) for name in ("stdout", "stderr")) if path is not None}

        # TODO group by a common prefix

        for position, path in enumerate(candidates):
            candidate = self.is_existing_path(self.working_dir / path)

            if candidate is None:
                raise errors.UsageError('Path "{0}" does not exist.'.format(path))

            glob = str(candidate.relative_to(self.working_dir))

            if glob in streams:
                continue

            new_input = None

            if glob in conflicting_paths:
                # it means that it is rewriting a file
                input = conflicting_paths[glob]
                new_input = attr.evolve(input, type="string", default=glob)
                input_candidates[glob] = new_input

                del conflicting_paths[glob]
                # TODO add warning ('Output already exists in inputs.')

            candidate_type = "Directory" if candidate.is_dir() else "File"

            if glob in input_candidates:
                input = input_candidates[glob]

                if new_input is None:
                    new_input = input_candidates[glob] = attr.evolve(input, type="string", default=glob)

                yield (
                    CommandOutputParameter(
                        id="output_{0}".format(position),
                        type=candidate_type,
                        outputBinding=dict(glob="$(inputs.{0})".format(input.id),),
                    ),
                    new_input,
                    glob,
                )
            else:
                yield (
                    CommandOutputParameter(
                        id="output_{0}".format(position), type=candidate_type, outputBinding=dict(glob=glob,),
                    ),
                    None,
                    glob,
                )

    def find_explicit_inputs(self):
        """Yield explicit inputs and command line input bindings if any."""
        input_paths = [input.default.path for input in self.inputs if input.type in PATH_OBJECTS]
        input_id = len(self.inputs) + len(self.arguments)

        for explicit_input in self.explicit_inputs:
            if explicit_input in input_paths:
                continue

            input_paths.append(explicit_input)

            try:
                explicit_input.relative_to(self.working_dir)
            except ValueError:
                raise errors.UsageError(
                    "The input file or directory is not in the repository."
                    "\n\n\t" + click.style(str(explicit_input), fg="yellow") + "\n\n"
                )
            if self.is_existing_path(explicit_input) is None:
                raise errors.UsageError(
                    "The input file or directory does not exist."
                    "\n\n\t" + click.style(str(explicit_input), fg="yellow") + "\n\n"
                )
            input_id += 1
            default, type, _ = self.guess_type(explicit_input)
            # Explicit inputs are either File or Directory
            assert type in PATH_OBJECTS
            # The inputBinging is None because these inputs won't
            # appear on command-line
            yield CommandInputParameter(id="input_{0}".format(input_id), type=type, default=default, inputBinding=None)

    def add_indirect_inputs(self):
        """Read indirect inputs list and add them to explicit inputs."""
        indirect_inputs_list = get_indirect_inputs_path(self.working_dir)

        for indirect_input in self._read_files_list(indirect_inputs_list):
            # treat indirect inputs like explicit inputs
            path = Path(os.path.abspath(indirect_input))
            self.explicit_inputs.append(path)

        # add new explicit inputs (if any) to inputs
        for input in self.find_explicit_inputs():
            self.inputs.append(input)

    def add_indirect_outputs(self):
        """Read indirect outputs list and add them to explicit outputs."""
        indirect_outputs_list = get_indirect_outputs_path(self.working_dir)

        for indirect_output in self._read_files_list(indirect_outputs_list):
            # treat indirect outputs like explicit outputs
            path = Path(os.path.abspath(indirect_output))
            self.explicit_outputs.append(path)

    def _include_indirect_parameters(self):
        run_parameters = read_indirect_parameters(self.working_dir)

        self.run_parameters = [RunParameter(name=k, value=v) for k, v in run_parameters.items()]

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
