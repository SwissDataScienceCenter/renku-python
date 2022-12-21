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
"""Converter for workflows to Renku workflow description."""

import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Union

from renku.core import errors
from renku.core.plugin import hookimpl
from renku.core.util import communication
from renku.core.util.os import get_absolute_path
from renku.core.util.yaml import write_yaml
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.converters import IWorkflowConverter
from renku.domain_model.workflow.plan import Plan


class RenkuWorkflowFileExporter(IWorkflowConverter):
    """Export a ``CompositePlan`` or a ``Plan`` to Renku workflow file."""

    @hookimpl
    def workflow_format(self):
        """Workflow format name."""
        return self, ["renku"]

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
        """Converts the specified workflow to renku workflow file."""

        def get_output_path():
            if output:
                path = output / workflow.name if output.is_dir() else output
            else:
                path = Path(tempfile.mkdtemp()) / workflow.name

            absolute_path = Path(get_absolute_path(path))
            if absolute_path.exists():
                communication.confirm(f"Output file '{path}' exists. Overwrite it?", abort=True, warning=True)

            return absolute_path

        output_path = get_output_path()

        if isinstance(workflow, CompositePlan):
            RenkuWorkflowFileExporter.convert_composite_plan(plan=workflow, path=output_path)
        else:
            RenkuWorkflowFileExporter.convert_plan(plan=workflow, path=output_path)

        return output_path.read_text()

    @staticmethod
    def convert_composite_plan(plan: CompositePlan, path: Path):
        """Converts a composite plan to a workflow file."""
        data: Dict[str, Any] = {
            "name": plan.name,
            "description": plan.description,
            "keywords": plan.keywords,
            "steps": [{p.name: RenkuWorkflowFileExporter.convert_step(step=p)} for p in plan.plans],
        }

        write_yaml(path=path, data=data)

    @staticmethod
    def convert_plan(plan: Plan, path: Path):
        """Converts a Plan to Renku workflow file format."""
        # NOTE: There is no root plan and we need to create one; its name can't be the same as step's (i.e. plan's) name
        data = {
            "name": f"{plan.name}-workflow-file",
            "steps": [{plan.name: RenkuWorkflowFileExporter.convert_step(step=plan)}],
        }
        write_yaml(path=path, data=data)

    @staticmethod
    def convert_step(step: Plan):
        """Converts a Plan to workflow file dict."""

        def convert_path(parameter):
            data = {"path": parameter.actual_value}
            if parameter.prefix:
                data["prefix"] = parameter.prefix.rstrip()
            if parameter.description:
                data["description"] = parameter.description

            return data

        def convert_parameter(parameter):
            data = {"value": parameter.actual_value}
            if parameter.prefix:
                data["prefix"] = parameter.prefix.rstrip()
            if parameter.description:
                data["description"] = parameter.description

            return data

        # NOTE: Check to make sure that the parent CompositePlan doesn't have a child CompositePlan
        if isinstance(step, CompositePlan):
            raise errors.WorkflowExportError(
                "Cannot export CompositePlans within a CompositePlan to Renku workflow file format"
            )

        step_data: Dict = {
            "command": " ".join(step.to_argv(with_streams=True)),
            "description": step.description,
            "keywords": step.keywords,
        }
        if step.inputs:
            step_data["inputs"] = [{p.name: convert_path(p)} for p in step.inputs]
        if step.outputs:
            step_data["outputs"] = [{p.name: convert_path(p)} for p in step.outputs]
        if step.parameters:
            step_data["parameters"] = [{p.name: convert_parameter(p)} for p in step.parameters]

        return step_data
