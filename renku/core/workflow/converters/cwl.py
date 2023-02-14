# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from uuid import uuid4

import cwl_utils.parser.cwl_v1_2 as cwl

from renku.core import errors
from renku.core.plugin import hookimpl
from renku.core.plugin.provider import RENKU_ENV_PREFIX
from renku.core.util.yaml import dumps_yaml, write_yaml
from renku.core.workflow.model.concrete_execution_graph import ExecutionGraph
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.converters import IWorkflowConverter
from renku.domain_model.workflow.parameter import DIRECTORY_MIME_TYPE, CommandInput, CommandOutput, CommandParameter
from renku.domain_model.workflow.plan import Plan


class CommandLineTool(cwl.CommandLineTool):
    """CommandLineTool that supports empty outputs."""

    def get_dict(self):
        """Set outputs to empty if not set."""
        d = super(CommandLineTool, self).get_dict()  # type: ignore[misc]
        if "outputs" not in d:
            d["outputs"] = []
        return d


class WorkflowStep(cwl.WorkflowStep):
    """WorkflowStep that supports empty outputs."""

    def get_dict(self):
        """Set out to empty if not set."""
        d = super(WorkflowStep, self).get_dict()  # type: ignore[misc]
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
        self,
        workflow: Union[CompositePlan, Plan],
        basedir: Path,
        output: Optional[Path],
        output_format: Optional[str],
        resolve_paths: Optional[bool],
        nest_workflows: Optional[bool],
    ) -> str:
        """Converts the specified workflow to CWL format.

        Args:
            worflow(Union[CompositePlan, Plan]): The plan or composite plan to be converted to cwl.
            basedir(Path): The path of the base location used as a prefix for all workflow input and outputs.
            output(Optional[Path]): The file where the CWL specification should be saved,
                if None then no file is created.
            output_format(Optional[str]): Not used. Only YAML is generated, regardless of what is provided.
            resolve_paths(Optional[bool]): Whether to make all paths absolute and resolve all symlinks,
                True by default.
            nest_workflows(Optional[bool]): Whether nested CWL workflows should be used or each sub-workflow should be
                a separate file, False by default.

        Returns:
            The contents of the CWL workflow as string. If nested workflows are used then only the parent
            specification is returned.
        """
        filename = None

        if resolve_paths is None:
            resolve_paths = True

        if output:
            if output.is_dir():
                tmpdir = output
                filename = None
            else:
                tmpdir = output.parent
                filename = output
        else:
            tmpdir = Path(tempfile.mkdtemp())

        cwl_workflow: Union[cwl.Workflow, CommandLineTool]
        if isinstance(workflow, CompositePlan):
            cwl_workflow = CWLExporter._convert_composite(workflow, basedir, resolve_paths=resolve_paths)
            if nest_workflows:
                # INFO: There is only one parent workflow with all children embedded in it
                if cwl_workflow.requirements is None:
                    cwl_workflow.requirements = []
                cwl_workflow.requirements.append(cwl.SubworkflowFeatureRequirement())
            else:
                # INFO: The parent composite worfklow references other workflow files,
                # write the child workflows in separate files and reference them in parent
                for step in cast(List[WorkflowStep], cwl_workflow.steps):
                    step_filename = Path(f"{uuid4()}.cwl")
                    step_path = (tmpdir / step_filename).resolve()
                    write_yaml(step_path, step.run.save())
                    step.run = str(step_path)
            if filename is None:
                filename = Path(f"parent_{uuid4()}.cwl")
        else:
            cwl_workflow = CWLExporter._convert_step(workflow, basedir, resolve_paths=resolve_paths)
            if filename is None:
                filename = Path(f"{uuid4()}.cwl")

        cwl_workflow_dict: Dict[str, Any] = cwl_workflow.save()
        path = (tmpdir / filename).resolve()
        write_yaml(path, cwl_workflow_dict)
        return dumps_yaml(cwl_workflow_dict)

    @staticmethod
    def _sanitize_id(id):
        return re.sub(r"/|-", "_", id)

    @staticmethod
    def _convert_composite(workflow: CompositePlan, basedir: Path, resolve_paths: bool) -> cwl.Workflow:
        """Converts a composite plan to a CWL file."""
        inputs: Dict[str, str] = {}
        arguments = {}
        outputs: Dict[str, Tuple[str, Any]] = {}
        consumed_outputs = set()
        steps = []

        input_index = 1
        argument_index = 1

        graph = ExecutionGraph([workflow], virtual_links=True)
        cycles = graph.cycles
        if cycles:
            cycles = [list(map(lambda x: x.name, cycle)) for cycle in cycles]
            raise errors.GraphCycleError(cycles)

        import networkx as nx

        for i, wf in enumerate(nx.topological_sort(graph.workflow_graph)):
            step_clitool = CWLExporter._convert_step(workflow=wf, basedir=basedir, resolve_paths=resolve_paths)
            step = WorkflowStep(in_=[], out=[], run=step_clitool, id="step_{}".format(i))

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
            location = Path(path)
            if resolve_paths:
                location = location.resolve()
                location_str = str(location.as_uri())
            else:
                location_str = str(location)
            workflow_object.inputs.append(
                cwl.WorkflowInputParameter(
                    id=id_,
                    type=type_,
                    default={"location": location_str, "class": type_},
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

        return workflow_object

    @staticmethod
    def _convert_step(workflow: Plan, basedir: Path, resolve_paths: bool) -> CommandLineTool:
        """Converts a single workflow step to a CWL CommandLineTool."""
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

            environment_variables.append(
                cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{output_.name}", str(output_.actual_value))
            )
            outp, arg = CWLExporter._convert_output(output_)
            tool_object.outputs.append(outp)
            if arg:
                tool_object.inputs.append(arg)

        for input_ in inputs:
            tool_input = CWLExporter._convert_input(input_, basedir, resolve_paths=resolve_paths)

            workdir_req.listing.append(
                cwl.Dirent(entry="$(inputs.{})".format(tool_input.id), entryname=input_.actual_value, writable=False)
            )

            environment_variables.append(
                cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{input_.name}", str(input_.actual_value))
            )
            tool_object.inputs.append(tool_input)
            if input_.mapped_to:
                tool_object.stdin = "$(inputs.{}.path)".format(tool_input.id)
                jsrequirement = True

        for parameter in workflow.parameters:
            environment_variables.append(
                cwl.EnvironmentDef(f"{RENKU_ENV_PREFIX}{parameter.name}", str(parameter.actual_value))
            )
            tool_object.inputs.append(CWLExporter._convert_parameter(parameter))

        workdir_req.listing.append(
            cwl.Dirent(entry="$(inputs.input_renku_metadata)", entryname=".renku", writable=False)
        )
        location = basedir / ".renku"
        if resolve_paths:
            location = location.resolve()
            location_str = location.as_uri()
        else:
            location_str = str(location)
        tool_object.inputs.append(
            cwl.CommandInputParameter(
                id="input_renku_metadata",
                type="Directory",
                inputBinding=None,
                default={"location": location_str, "class": "Directory"},
            )
        )

        if workdir_req.listing:
            tool_object.requirements.append(workdir_req)  # type: ignore
        if jsrequirement:
            tool_object.requirements.append(cwl.InlineJavascriptRequirement())  # type: ignore
        if environment_variables:
            tool_object.requirements.append(cwl.EnvVarRequirement(environment_variables))  # type: ignore

        return tool_object

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
    def _convert_input(input: CommandInput, basedir: Path, resolve_paths: bool):
        """Converts an input to a CWL input."""
        type_ = (
            "Directory"
            if input.encoding_format is not None and DIRECTORY_MIME_TYPE in input.encoding_format
            else "File"
        )
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

        location = basedir / input.actual_value
        if resolve_paths:
            location = location.resolve()
            location_str = location.as_uri()
        else:
            location_str = str(location)
        return cwl.CommandInputParameter(
            id=sanitized_id,
            type=type_,
            inputBinding=cwl.CommandLineBinding(position=position, prefix=prefix, separate=separate)
            if position or prefix
            else None,
            default={"location": location_str, "class": type_},
        )

    @staticmethod
    def _convert_output(output: CommandOutput):
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

        type_ = (
            "Directory"
            if output.encoding_format is not None and DIRECTORY_MIME_TYPE in output.encoding_format
            else "File"
        )

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
