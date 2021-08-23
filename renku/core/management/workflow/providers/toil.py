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
"""toil based provider."""

import os
import shutil
import sys
import tempfile
import networkx as nx
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote
from abc import abstractmethod

from toil.common import Toil
from toil.job import Job
from toil.leader import FailedJobsException
from toil.fileStores import FileID

from renku.core import errors
from renku.core.commands.echo import progressbar
from renku.core.errors import WorkflowExecuteError
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import AbstractPlan
from renku.core.models.workflow.provider import IWorkflowProvider
from renku.core.plugins import hookimpl
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph

from subprocess import call


class AbstractToilJob(Job):
    def __init__(self, workflow: AbstractPlan, input_files: Dict[str, FileID], *args, **kwargs):
        super(AbstractToilJob, self).__init__(*args, **kwargs)
        self._workflow = workflow
        self._input_files: Dict[str, FileID] = input_files

    @abstractmethod
    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        pass

    def run(self, fileStore):
        mapped_std = dict()
        for i in self._workflow.inputs:
            fileStore.readGlobalFile(self._input_files[i.actual_value], i.actual_value)
            if i.mapped_to:
                mapped_std[i.mapped_to.stream_type] = i.actual_value

        for o in self._workflow.outputs:
            if o.mapped_to:
                mapped_std[o.mapped_to.stream_type] = o.actual_value

        # construct cmd
        cmd = []

        if self._workflow.command:
            cmd.extend(self._workflow.command.split(" "))

        arguments = self._workflow.inputs + self._workflow.outputs + self._workflow.parameters

        arguments = filter(lambda x: x.position, arguments)
        arguments = sorted(arguments, key=lambda x: x.position)
        cmd.extend(e for a in arguments for e in a.to_argv())

        return_code = self._execute(cmd, mapped_std)
        if return_code not in (self._workflow.success_codes or {0}):
            raise errors.InvalidSuccessCode(return_code, success_codes=self._workflow.success_codes)

        output_fids = dict()
        for o in self._workflow.outputs:
            output_fids[o.actual_value] = fileStore.writeGlobalFile(o.actual_value)

        return output_fids


class SubprocessToilJob(AbstractToilJob):
    def __init__(self, workflow: AbstractPlan, input_files: Dict[str, FileID], *args, **kwargs):
        super(SubprocessToilJob, self).__init__(workflow, input_files, *args, **kwargs)

    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        return call(
            command_line,
            cwd=os.getcwd(),
            **{key: open(value, mode="r" if key == "stdin" else "w") for key, value in mapped_std.items()},
        )


class ToilProvider(IWorkflowProvider):
    """A workflow executor provider using toil."""

    @hookimpl
    def workflow_provider(self):
        """Workflow provider name."""
        return (self, "toil")

    @hookimpl
    def workflow_execute(self, workflow: AbstractPlan, basedir: Path, config_file: Optional[str]):
        tmpdir = tempfile.mkdtemp()
        options = Job.Runner.getDefaultOptions(os.path.join(tmpdir, workflow.name))
        options.logLevel = "DEBUG"
        options.clean = "always"

        outputs = list()
        try:
            with Toil(options) as toil:
                root_job = None
                if isinstance(workflow, CompositePlan):
                    graph = ExecutionGraph(workflow, virtual_links=True)
                    cycles = graph.cycles
                    if cycles:
                        cycles = [map(lambda x: x.name, cycle) for cycle in cycles]
                        raise errors.GraphCycleError(cycles)

                    # TODO: finish up with Promises passed down to children
                    root_job = None
                    parent_job = None
                    output_fid = dict()
                    for w in nx.dfs_preorder_nodes(graph.workflow_graph):
                        job = self._create_job(w, toil, basedir)
                        if not root_job:
                            parent_job = root_job = job
                        else:
                            parent_job.addChild(job)
                        output_fid = job.rv()
                        parent_job = job
                else:
                    root_job = self._create_job(workflow, toil, basedir)
                outputs = toil.start(root_job)

                with progressbar(length=len(outputs), label="Moving outputs") as bar:
                    for name, fid in outputs.items():
                        toil.exportFile(fid, (basedir / name).absolute().as_uri())
                        bar.update(1)
        except FailedJobsException:
            raise WorkflowExecuteError()
        finally:
            shutil.rmtree(tmpdir)

        return outputs.values()

    def _create_job(self, workflow: AbstractPlan, toil: Toil, basedir: Path):
        if isinstance(workflow, CompositePlan):
            # TODO: handle mappings and links
            raise Exception("TODO")
        else:
            input_files = dict()
            for i in workflow.inputs:
                input_files[i.actual_value] = toil.importFile(str((basedir / i.actual_value).as_uri()))

            return SubprocessToilJob(workflow, input_files)
