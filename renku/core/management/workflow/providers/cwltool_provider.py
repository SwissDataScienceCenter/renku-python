# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""cwltool based provider."""

import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import unquote

import cwltool.factory
from cwltool.context import LoadingContext, RuntimeContext

from renku.core.commands.echo import progressbar
from renku.core.errors import WorkflowExecuteError
from renku.core.models.workflow.plan import AbstractPlan
from renku.core.models.workflow.provider import IWorkflowProvider
from renku.core.plugins import hookimpl
from renku.core.plugins.workflow import workflow_converter


class CWLToolProvider(IWorkflowProvider):
    """A workflow executor provider using cwltool."""

    @hookimpl
    def workflow_provider(self):
        """Workflow provider name."""
        return (self, "cwltool")

    @hookimpl
    def workflow_execute(self, workflow: AbstractPlan, basedir: Path, config_file: Optional[str]):
        """Executes a given workflow using cwltool."""
        with tempfile.NamedTemporaryFile() as f:
            # export Plan to cwl
            converter = workflow_converter("cwl")
            converter(workflow=workflow, basedir=basedir, output=Path(f.name), output_format=None)

            # TODO: pass parameters to cwltool
            if config_file:
                raise Exception("needs implementation")

            # run cwl with cwltool
            argv = sys.argv
            sys.argv = ["cwltool"]

            # Keep all environment variables.
            runtime_context = RuntimeContext(
                kwargs={"rm_tmpdir": False, "move_outputs": "leave", "preserve_entire_environment": True}
            )

            loading_context = LoadingContext(kwargs={"relax_path_checks": True})

            factory = cwltool.factory.Factory(loading_context=loading_context, runtime_context=runtime_context)
            process = factory.make(os.path.relpath(str(f.name)))

            try:
                outputs = process()
            except cwltool.factory.WorkflowStatus:
                raise WorkflowExecuteError()

            sys.argv = argv

            # Move outputs to correct location in the repository.
            output_dirs = process.factory.executor.output_dirs

            def remove_prefix(location, prefix="file://"):
                if location.startswith(prefix):
                    return unquote(location[len(prefix) :])
                return unquote(location)

            locations = {remove_prefix(output["location"]) for output in outputs.values()}
            # make sure to not move an output if it's containing directory gets moved
            locations = {
                location
                for location in locations
                if not any(location.startswith(d) for d in locations if location != d)
            }

            output_paths = []
            with progressbar(locations, label="Moving outputs") as bar:
                for location in bar:
                    for output_dir in output_dirs:
                        if location.startswith(output_dir):
                            output_path = location[len(output_dir) :].lstrip(os.path.sep)
                            destination = basedir / output_path
                            output_paths.append(destination)
                            if destination.is_dir():
                                shutil.rmtree(str(destination))
                                destination = destination.parent
                            shutil.move(location, str(destination))
                            continue

            return output_paths
