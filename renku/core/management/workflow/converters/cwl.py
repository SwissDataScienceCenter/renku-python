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
"""Converter for workflows to CWL."""

import os
import re
import tempfile
from pathlib import Path
from typing import Optional, Union
from uuid import uuid4

import cwl_utils.parser.cwl_v1_2 as cwl

from renku.core import errors
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.models.jsonld import write_yaml
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.converters import IWorkflowConverter
from renku.core.models.workflow.parameter import DIRECTORY_MIME_TYPE, CommandInput, CommandParameter
from renku.core.models.workflow.plan import Plan
from renku.core.plugins import hookimpl
from renku.core.plugins.provider import RENKU_ENV_PREFIX


class CommandLineTool(cwl.CommandLineTool):
    """CommandLineTool that supports empty outputs."""

    def get_dict(self):
        """Set outputs to empty if not set."""
        d = super(CommandLineTool, self).get_dict()
        if "outputs" not in d:
            d["outputs"] = []
        return d


class WorkflowStep(cwl.WorkflowStep):
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

    if isinstance(value, float):
        type_ = "float"
    elif isinstance(value, int):
        type_ = "int"
    else:
        try:
            casted_value = float(value)

            if "." not in value:
                type_ = "int"
                casted_value = int(value)
            else:
                type_ = "float"

            value = casted_value
        except ValueError:
            pass

    return value, type_


class CWLExporter(IWorkflowConverter):
    """Converts a ``CompositePlan`` or a ``Plan`` to CWL format."""

    @hookimpl
    def workflow_format(self):
        """Workflow format name."""
        return (self, ["cwl"])

    @hookimpl
    def workflow_convert(
        self, workflow: Union[CompositePlan, Plan], basedir: Path, output: Optional[Path], output_format: Optional[str]
    ):
        """Converts the specified workflow to CWL format."""
        filename = None
        if output:
            if output.is_dir():
                tmpdir = output
            else:
                tmpdir = output.parent
                filename = output
        else:
            tmpdir = Path(tempfile.mkdtemp())

        if isinstance(workflow, CompositePlan):
            path = CWLExporter._convert_composite(
                workflow, tmpdir, basedir, filename=filename, output_format=output_format
            )
        else:
            _, path = CWLExporter._convert_step(
                workflow, tmpdir, basedir, filename=filename, output_format=output_format
            )

        return path.read_text()

    @staticmethod
    def _sanitize_id(id):
        return re.sub(r"/|-", "_", id)

    @staticmethod
    def _convert_composite(
        workflow: CompositePlan, tmpdir: Path, basedir: Path, filename: Optional[Path], output_format: Optional[str]
    ):
        """Converts a composite plan to a CWL file."""
        inputs = {}
        arguments = {}
        outputs = {}
        consumed_outputs = set()
        steps = []

        input_index = 1
        argument_index = 1

        graph = ExecutionGraph([workflow], virtual_links=True)
        cycles = graph.cycles
        if cycles:
            cycles = [map(lambda x: x.name, cycle) for cycle in cycles]
            raise errors.GraphCycleError(cycles)

        import networkx as nx

        for i, wf in enumerate(nx.topological_sort(graph.workflow_graph)):
            cwl_workflow, path = CWLExporter._convert_step(
                workflow=wf, tmpdir=tmpdir, basedir=basedir, filename=None, output_format=output_format
            )
            step = WorkflowStep(in_=[], out=[], run=str(path), id="step_{}".format(i))

            for input in wf.inputs:
                input_path = input.actual_value

                sanitized_id = CWLExporter._sanitize_id(input.id)
                if input_path in inputs:
                    # already used as top-level input elsewhere, reuse
                    step.in_.append(cwl.WorkflowStepInput(sanitized_id, source=inputs[input_path]))
                elif input_path in outputs:
                    # output of a previous step, refer to it
                    consumed_outputs.add(outputs[input_path][0])
                    step.in_.append(
                        cwl.WorkflowStepInput(
                            sanitized_id, source="{}/{}".format(outputs[input_path][1], outputs[input_path][0])
                        )
                    )
                else:
                    # input isn't output and doesn't exist yet, add new
                    inputs[input_path] = "input_{}".format(input_index)
                    step.in_.append(cwl.WorkflowStepInput(sanitized_id, source=inputs[input_path]))
                    input_index += 1

            for parameter in wf.parameters:
                argument_id = "argument_{}".format(argument_index)
                arguments[argument_id] = parameter.actual_value
                step.in_.append(cwl.WorkflowStepInput(CWLExporter._sanitize_id(parameter.id), source=argument_id))
                argument_index += 1

            for output in wf.outputs:
                sanitized_id = CWLExporter._sanitize_id(output.id)

                if output.mapped_to:
                    sanitized_id = "output_{}".format(output.mapped_to.stream_type)
                outputs[output.actual_value] = (sanitized_id, step.id)
                step.out.append(cwl.WorkflowStepOutput(sanitized_id))

            steps.append(step)

        workflow_object = cwl.Workflow([], [], steps, id=str(uuid4()), requirements=[], hints=[], cwlVersion="v1.0")

        # check types of paths and add as top level inputs/outputs
        for path, id_ in inputs.items():
            type_ = "Directory" if os.path.isdir(path) else "File"
            workflow_object.inputs.append(
                cwl.WorkflowInputParameter(
                    id=id_,
                    type=type_,
                    default={"location": Path(path).resolve().as_uri(), "class": type_},
                )
            )

        for id_, value in arguments.items():
            value, type_ = _get_argument_type(value)
            workflow_object.inputs.append(cwl.WorkflowInputParameter(id=id_, type=type_, default=value))

        for index, (path, (id_, step_id)) in enumerate(outputs.items(), 1):
            type_ = "Directory" if os.path.isdir(path) else "File"
            workflow_object.outputs.append(
                cwl.WorkflowOutputParameter(
                    id="output_{}".format(index), outputSource="{}/{}".format(step_id, id_), type=type_
                )
            )
        if not filename:
            filename = "parent_{}.cwl".format(uuid4())

        output = workflow_object.save()
        path = (tmpdir / filename).resolve()
        write_yaml(path, output)
        return path

    @staticmethod
    def _convert_step(
        workflow: Plan, tmpdir: Path, basedir: Path, filename: Optional[Path], output_format: Optional[str]
    ):
        """Converts a single workflow step to a CWL file."""
        stdin, stdout, stderr = None, None, None

        inputs = list(workflow.inputs)

        for output_ in workflow.outputs:
            if not output_.mapped_to:
                continue
            if output_.mapped_to.stream_type == "stderr":
                stderr = output_.actual_value
            if output_.mapped_to.stream_type == "stdout":
                stdout = output_.actual_value

        tool_object = CommandLineTool(
            inputs=[],
            outputs=[],
            id=str(uuid4()),
            baseCommand=workflow.command.split(" "),
            requirements=[],
            stdin=stdin,
            stderr=stderr,
            stdout=stdout,
            cwlVersion="v1.0",
        )

        workdir_req = cwl.InitialWorkDirRequirement([])
        jsrequirement = False

        environment_variables = []
        dirents = []

        for output_ in workflow.outputs:
            path = output_.actual_value
            if not os.path.isdir(path):
                path = str(Path(path).parent)
            if path != "." and path not in dirents and output_.create_folder:
                # workflow needs to create subdirectory for output file,
                # if the directory was not already present
                workdir_req.listing.append(
                    cwl.Dirent(entry='$({"listing": [], "class": "Directory"})', entryname=path, writable=True)
                )
                dirents.append(path)
                jsrequirement = True

            environment_variables.append(cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{output_.name}", output_.actual_value))
            outp, arg = CWLExporter._convert_output(output_)
            tool_object.outputs.append(outp)
            if arg:
                tool_object.inputs.append(arg)

        for input_ in inputs:
            tool_input = CWLExporter._convert_input(input_, basedir)

            workdir_req.listing.append(
                cwl.Dirent(entry="$(inputs.{})".format(tool_input.id), entryname=input_.actual_value, writable=False)
            )

            environment_variables.append(cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{input_.name}", input_.actual_value))
            tool_object.inputs.append(tool_input)
            if input_.mapped_to:
                tool_object.stdin = "$(inputs.{}.path)".format(tool_input.id)
                jsrequirement = True

        for parameter in workflow.parameters:
            environment_variables.append(
                cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{parameter.name}", parameter.actual_value)
            )
            tool_object.inputs.append(CWLExporter._convert_parameter(parameter))

        workdir_req.listing.append(
            cwl.Dirent(entry="$(inputs.input_renku_metadata)", entryname=".renku", writable=False)
        )
        tool_object.inputs.append(
            cwl.CommandInputParameter(
                id="input_renku_metadata",
                type="Directory",
                inputBinding=None,
                default={"location": (basedir / ".renku").resolve().as_uri(), "class": "Directory"},
            )
        )

        if workdir_req.listing:
            tool_object.requirements.append(workdir_req)
        if jsrequirement:
            tool_object.requirements.append(cwl.InlineJavascriptRequirement())
        if environment_variables:
            tool_object.requirements.append(cwl.EnvVarRequirement(environment_variables))

        output = tool_object.save()
        if not filename:
            filename = "{}.cwl".format(uuid4())
        path = (tmpdir / filename).resolve()
        write_yaml(path, output)
        return output, path

    @staticmethod
    def _convert_parameter(parameter: CommandParameter):
        """Converts an parameter to a CWL input."""
        value, type_ = _get_argument_type(parameter.actual_value)

        separate = None
        prefix = None
        if parameter.prefix:
            prefix = parameter.prefix
            separate = False

            if prefix.endswith(" "):
                prefix = prefix[:-1]
                separate = True

        return cwl.CommandInputParameter(
            id=CWLExporter._sanitize_id(parameter.id),
            type=type_,
            inputBinding=cwl.CommandLineBinding(position=parameter.position, prefix=prefix, separate=separate)
            if parameter.position or prefix
            else None,
            default=value,
        )

    @staticmethod
    def _convert_input(input: CommandInput, basedir: Path):
        """Converts an input to a CWL input."""
        type_ = "Directory" if DIRECTORY_MIME_TYPE in input.encoding_format else "File"
        position = input.position

        sanitized_id = CWLExporter._sanitize_id(input.id)
        if input.mapped_to:
            sanitized_id = "input_stdin"
            position = None

        separate = None
        prefix = None
        if input.prefix:
            prefix = input.prefix
            separate = False

            if prefix.endswith(" "):
                prefix = prefix[:-1]
                separate = True

        return cwl.CommandInputParameter(
            id=sanitized_id,
            type=type_,
            inputBinding=cwl.CommandLineBinding(position=position, prefix=prefix, separate=separate)
            if position or prefix
            else None,
            default={"location": (basedir / input.actual_value).resolve().as_uri(), "class": type_},
        )

    @staticmethod
    def _convert_output(output: CommandInput):
        """Converts an output to a CWL output."""
        if output.mapped_to:
            return (
                cwl.CommandOutputParameter(
                    id="output_{}".format(output.mapped_to.stream_type),
                    type=output.mapped_to.stream_type,
                    streamable=False,
                ),
                None,
            )

        type_ = "Directory" if DIRECTORY_MIME_TYPE in output.encoding_format else "File"

        sanitized_id = CWLExporter._sanitize_id(output.id)

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

            arg = cwl.CommandInputParameter(
                id="{}_arg".format(sanitized_id),
                type="string",
                inputBinding=cwl.CommandLineBinding(position=output.position, prefix=prefix, separate=separate),
                default=output.actual_value,
            )
            outp = cwl.CommandOutputParameter(
                id=sanitized_id,
                type=type_,
                outputBinding=cwl.CommandOutputBinding(glob="$(inputs.{})".format(arg.id)),
            )
            return outp, arg

        return (
            cwl.CommandOutputParameter(
                id=sanitized_id, type=type_, outputBinding=cwl.CommandOutputBinding(glob=output.actual_value)
            ),
            None,
        )
