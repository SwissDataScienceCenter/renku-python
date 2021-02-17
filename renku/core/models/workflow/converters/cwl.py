# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Converter for workflows to cwl."""

import os
import tempfile
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

import cwlgen

from renku.core.models.entities import Collection
from renku.core.models.workflow.parameters import CommandOutput


class CommandLineTool(cwlgen.CommandLineTool):
    """CommandLineTool that supports empty outputs."""

    def get_dict(self):
        """Set outputs to empty if not set."""
        d = super(CommandLineTool, self).get_dict()
        if "outputs" not in d:
            d["outputs"] = []
        return d


class WorkflowStep(cwlgen.WorkflowStep):
    """WorkflowStep that supports empty outputs."""

    def get_dict(self):
        """Set out to empty if not set."""
        d = super(WorkflowStep, self).get_dict()
        if "out" not in d:
            d["out"] = []
        return d


def _get_argument_type(value):
    """Get the type of a command line argument."""
    type_ = "string"

    try:
        value = int(value)
        type_ = "int"
    except ValueError:
        pass

    return value, type_


def _recurse_subprocesses(run, index):
    """Recursively get actual steps (not ``WorkflowRun``) for execution."""
    if not run.subprocesses:
        return [(index, run)], index + 1

    processes = []
    for s in sorted(run.subprocesses, key=lambda x: x.index):
        result, index = _recurse_subprocesses(s.process, index)
        processes.extend(result)

    return processes, index


class CWLConverter(object):
    """Converts a ``Run`` to cwl file(s)."""

    @staticmethod
    def convert(run, client, path=None):
        """Convert the workflow to one ore more .cwl files."""
        basedir = client.path
        filename = None
        if path:
            if os.path.isdir(path):
                tmpdir = path
            else:
                tmpdir = path.parent
                filename = path
        else:
            tmpdir = tempfile.mkdtemp()

        if hasattr(run, "subprocesses") and run.subprocesses:
            return CWLConverter._convert_composite(run, tmpdir, basedir, filename=filename)
        else:
            return CWLConverter._convert_step(run, tmpdir, basedir, filename=filename)

    @staticmethod
    def _convert_composite(run, tmpdir, basedir, filename=None):
        """Converts a workflow made up of several steps."""
        inputs = {}
        arguments = {}
        outputs = {}
        consumed_outputs = set()
        steps = []

        input_index = 1
        argument_index = 1

        subprocesses, _ = _recurse_subprocesses(run, 1)

        # preprocess to add dummy outputs in case of output directories
        previous_output_dirs = defaultdict(list)
        for _, subprocess in subprocesses:
            for input in subprocess.inputs:
                entity = input.consumes
                key = (entity.commit.hexsha, entity.path)
                if key not in previous_output_dirs:
                    continue

                for previous_process in previous_output_dirs[key]:
                    new_output = CommandOutput(
                        id=CommandOutput.generate_id(subprocess._id), produces=entity, create_folder=False
                    )
                    previous_process.outputs.append(new_output)

            for output in subprocess.outputs:
                entity = output.produces
                if not isinstance(entity, Collection):
                    continue

                for e in entity.entities:
                    if e.commit.hexsha != entity.commit.hexsha:
                        continue

                    key = (e.commit.hexsha, e.path)
                    previous_output_dirs[key].append(subprocess)

        # Convert workflow steps
        for i, subprocess in subprocesses:
            tool, path = CWLConverter._convert_step(subprocess, tmpdir, basedir)
            step = WorkflowStep("step_{}".format(i), path)

            for input in subprocess.inputs:
                input_path = input.consumes.path

                sanitized_id = input.sanitized_id.replace("/", "_")
                if input_path in inputs:
                    # already used as top-level input elsewhere, reuse
                    step.inputs.append(cwlgen.WorkflowStepInput(sanitized_id, source=inputs[input_path]))
                elif input_path in outputs:
                    # output of a previous step, refer to it
                    consumed_outputs.add(outputs[input_path][0])
                    step.inputs.append(
                        cwlgen.WorkflowStepInput(
                            sanitized_id, source="{}/{}".format(outputs[input_path][1], outputs[input_path][0])
                        )
                    )
                else:
                    # input isn't output and doesn't exist yet, add new
                    inputs[input_path] = "input_{}".format(input_index)
                    step.inputs.append(cwlgen.WorkflowStepInput(sanitized_id, source=inputs[input_path]))
                    input_index += 1

            for argument in subprocess.arguments:
                argument_id = "argument_{}".format(argument_index)
                arguments[argument_id] = argument.value
                step.inputs.append(
                    cwlgen.WorkflowStepInput(argument.sanitized_id.replace("/", "_"), source=argument_id)
                )
                argument_index += 1

            for output in subprocess.outputs:
                sanitized_id = output.sanitized_id.replace("/", "_")

                if output.mapped_to:
                    sanitized_id = "output_{}".format(output.mapped_to.stream_type)
                outputs[output.produces.path] = (sanitized_id, step.id)
                step.out.append(cwlgen.WorkflowStepOutput(sanitized_id))

            steps.append(step)

        workflow_object = cwlgen.Workflow(str(uuid4()), cwl_version="v1.0")
        workflow_object.hints = []
        workflow_object.requirements = []

        # check types of paths and add as top level inputs/outputs
        for path, id_ in inputs.items():
            type_ = "Directory" if os.path.isdir(path) else "File"
            workflow_object.inputs.append(
                cwlgen.InputParameter(
                    id_,
                    param_type=type_,
                    default={"path": os.path.abspath(os.path.join(basedir, path)), "class": type_},
                )
            )

        for id_, value in arguments.items():
            value, type_ = _get_argument_type(value)
            workflow_object.inputs.append(cwlgen.InputParameter(id_, param_type=type_, default=value))

        for index, (path, (id_, step_id)) in enumerate(outputs.items(), 1):
            type_ = "Directory" if os.path.isdir(path) else "File"
            workflow_object.outputs.append(
                cwlgen.WorkflowOutputParameter(
                    "output_{}".format(index), output_source="{}/{}".format(step_id, id_), param_type=type_
                )
            )
        workflow_object.steps.extend(steps)
        if not filename:
            filename = "parent_{}.cwl".format(uuid4())
        path = os.path.join(tmpdir, filename)
        workflow_object.export(path)

        return workflow_object, path

    @staticmethod
    def _convert_step(step, tmpdir, basedir, filename=None):
        """Converts a single workflow step to a cwl file."""
        stdin, stdout, stderr = None, None, None

        inputs = list(step.inputs)

        for output in step.outputs:
            if not output.mapped_to:
                continue
            if output.mapped_to.stream_type == "stderr":
                stderr = output.produces.path
            if output.mapped_to.stream_type == "stdout":
                stdout = output.produces.path

        tool_object = CommandLineTool(
            tool_id=str(uuid4()),
            base_command=step.command.split(" "),
            stdin=stdin,
            stderr=stderr,
            stdout=stdout,
            cwl_version="v1.0",
        )

        workdir_req = cwlgen.InitialWorkDirRequirement([])
        jsrequirement = False

        dirents = []

        for output in step.outputs:
            path = output.produces.path
            if not os.path.isdir(path):
                path = str(Path(path).parent)
            if path != "." and path not in dirents and output.create_folder:
                # workflow needs to create subdirectory for output file,
                # if the directory was not already present
                workdir_req.listing.append(
                    cwlgen.InitialWorkDirRequirement.Dirent(
                        entry='$({"listing": [], "class": "Directory"})', entryname=path, writable=True
                    )
                )
                dirents.append(path)
                jsrequirement = True
            outp, arg = CWLConverter._convert_output(output)
            tool_object.outputs.append(outp)
            if arg:
                tool_object.inputs.append(arg)

        for input_ in inputs:
            tool_input = CWLConverter._convert_input(input_, basedir)

            workdir_req.listing.append(
                cwlgen.InitialWorkDirRequirement.Dirent(
                    entry="$(inputs.{})".format(tool_input.id), entryname=input_.consumes.path, writable=False
                )
            )

            tool_object.inputs.append(tool_input)
            if input_.mapped_to:
                tool_object.stdin = "$(inputs.{}.path)".format(tool_input.id)
                jsrequirement = True
        for argument in step.arguments:
            tool_object.inputs.append(CWLConverter._convert_argument(argument))

        workdir_req.listing.append(
            cwlgen.InitialWorkDirRequirement.Dirent(
                entry="$(inputs.input_renku_metadata)", entryname=".renku", writable=False
            )
        )
        tool_object.inputs.append(
            cwlgen.CommandInputParameter(
                "input_renku_metadata",
                param_type="Directory",
                input_binding=None,
                default={"path": os.path.abspath(os.path.join(basedir, ".renku")), "class": "Directory"},
            )
        )

        # TODO: ".git" is not required once https://github.com/SwissDataScienceCenter/renku-python/issues/1043 is done
        # because we won't need the git history to correctly load metadata. The following two statements can be removed.
        workdir_req.listing.append(
            cwlgen.InitialWorkDirRequirement.Dirent(
                entry="$(inputs.input_git_directory)", entryname=".git", writable=False
            )
        )
        tool_object.inputs.append(
            cwlgen.CommandInputParameter(
                "input_git_directory",
                param_type="Directory",
                input_binding=None,
                default={"path": os.path.abspath(os.path.join(basedir, ".git")), "class": "Directory"},
            )
        )

        if workdir_req.listing:
            tool_object.requirements.append(workdir_req)
        if jsrequirement:
            tool_object.requirements.append(cwlgen.InlineJavascriptRequirement())

        if not filename:
            filename = "{}.cwl".format(uuid4())
        path = os.path.join(tmpdir, filename)
        tool_object.export(path)

        return tool_object, path

    @staticmethod
    def _convert_input(input, basedir):
        """Converts an input to a CWL input."""
        entity = input.consumes
        type_ = "Directory" if isinstance(entity, Collection) else "File"

        sanitized_id = input.sanitized_id
        sanitized_id = sanitized_id.replace("/", "_")
        if input.mapped_to:
            sanitized_id = "input_stdin"

        separate = None
        prefix = None
        if input.prefix:
            prefix = input.prefix
            separate = False

            if prefix.endswith(" "):
                prefix = prefix[:-1]
                separate = True

        return cwlgen.CommandInputParameter(
            sanitized_id,
            param_type=type_,
            input_binding=cwlgen.CommandLineBinding(position=input.position, prefix=prefix, separate=separate),
            default={"path": os.path.abspath(os.path.join(basedir, entity.path)), "class": type_},
        )

    @staticmethod
    def _convert_output(output):
        """Converts an output to a CWL output."""
        if output.mapped_to:
            return (
                cwlgen.CommandOutputParameter(
                    "output_{}".format(output.mapped_to.stream_type),
                    param_type=output.mapped_to.stream_type,
                    streamable=False,
                ),
                None,
            )

        entity = output.produces
        type_ = "Directory" if isinstance(entity, Collection) else "File"

        sanitized_id = output.sanitized_id.replace("/", "_")

        if output.position:
            # output is specified as a parameter, create an input as well
            separate = None
            prefix = None
            if output.prefix:
                prefix = output.prefix
                separate = False

                if prefix.endswith(" "):
                    prefix = prefix[:-1]
                    separate = True

            arg = cwlgen.CommandInputParameter(
                "{}_arg".format(sanitized_id),
                param_type="string",
                input_binding=cwlgen.CommandLineBinding(position=output.position, prefix=prefix, separate=separate),
                default=entity.path,
            )
            outp = cwlgen.CommandOutputParameter(
                sanitized_id,
                param_type=type_,
                output_binding=cwlgen.CommandOutputBinding(glob="$(inputs.{})".format(arg.id)),
            )
            return outp, arg

        return (
            cwlgen.CommandOutputParameter(
                sanitized_id, param_type=type_, output_binding=cwlgen.CommandOutputBinding(glob=entity.path)
            ),
            None,
        )

    @staticmethod
    def _convert_argument(argument):
        """Converts an argument to a CWL input."""
        value, type_ = _get_argument_type(argument.value)

        separate = None
        prefix = None
        if argument.prefix:
            prefix = argument.prefix
            separate = False

            if prefix.endswith(" "):
                prefix = prefix[:-1]
                separate = True

        return cwlgen.CommandInputParameter(
            argument.sanitized_id.replace("/", "_"),
            param_type=type_,
            input_binding=cwlgen.CommandLineBinding(position=argument.position, prefix=prefix, separate=separate),
            default=value,
        )
