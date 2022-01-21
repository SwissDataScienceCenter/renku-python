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

import functools
import itertools
import os
import shutil
import uuid
from abc import abstractmethod
from pathlib import Path
from subprocess import call
from typing import Any, Callable, Dict, List

import networkx as nx
from toil.common import Toil
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job, Promise
from toil.leader import FailedJobsException

from renku.core import errors
from renku.core.commands.echo import progressbar
from renku.core.errors import WorkflowExecuteError
from renku.core.management.config import RENKU_HOME
from renku.core.management.workflow.plan_factory import RENKU_TMP
from renku.core.models.workflow.parameter import CommandParameterBase
from renku.core.models.workflow.plan import Plan
from renku.core.models.workflow.provider import IWorkflowProvider
from renku.core.plugins import hookimpl
from renku.core.plugins.provider import RENKU_ENV_PREFIX


class AbstractToilJob(Job):
    """Toil job implementation for a renku ``Plan``."""

    def __init__(self, workflow: Plan, *args, **kwargs):
        super(AbstractToilJob, self).__init__(unitName=workflow.name, displayName=workflow.name, *args, **kwargs)
        self.workflow = workflow
        self._input_files: Dict[str, FileID] = {}
        self._parents_promise = []
        self._environment = os.environ.copy()

    @abstractmethod
    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        raise NotImplementedError

    def set_input_files(self, input_files: Dict[str, Any]):
        """Set the location of inputs that are available in the original project."""
        self._input_files = input_files

    def add_input_promise(self, promise: Promise):
        """Adds a given job's output promise.

        The promise of a job contains its output locations in the global storage.
        """
        self._parents_promise.append(promise)

    def run(self, storage):
        """Executing of a renku ``Plan``."""
        mapped_std = dict()
        parent_inputs = dict()
        for p in self._parents_promise:
            parent_inputs.update(p)

        def _read_input(input: str, file_metadata):
            input_path = Path(input)

            if isinstance(file_metadata, dict):
                input_path.mkdir(parents=True, exist_ok=True)
                for path, file_id in file_metadata.items():
                    _read_input(path, file_id)
            elif not input_path.exists():
                if len(input_path.parts) > 1:
                    input_path.parent.mkdir(parents=True, exist_ok=True)
                storage.readGlobalFile(file_metadata, userPath=input)

        for i in self.workflow.inputs:
            file_metadata = (
                parent_inputs[i.actual_value] if i.actual_value in parent_inputs else self._input_files[i.actual_value]
            )
            _read_input(i.actual_value, file_metadata)

            self._environment[f"{RENKU_ENV_PREFIX}{i.name}"] = i.actual_value

            if i.mapped_to:
                mapped_std[i.mapped_to.stream_type] = i.actual_value

        for o in self.workflow.outputs:
            self._environment[f"{RENKU_ENV_PREFIX}{o.name}"] = o.actual_value
            output_path = Path(o.actual_value)
            if len(output_path.parts) > 1:
                output_path.parent.mkdir(parents=True, exist_ok=True)

            if o.mapped_to:
                mapped_std[o.mapped_to.stream_type] = o.actual_value

        for p in self.workflow.parameters:
            self._environment[f"{RENKU_ENV_PREFIX}{p.name}"] = p.actual_value

        # construct cmd
        cmd = []

        if self.workflow.command:
            cmd.extend(self.workflow.command.split(" "))

        arguments = itertools.chain(self.workflow.inputs, self.workflow.outputs, self.workflow.parameters)

        arguments = filter(lambda x: x.position and not getattr(x, "mapped_to", None), arguments)
        arguments = sorted(arguments, key=lambda x: x.position)

        for a in arguments:
            v = str(a.actual_value) if not isinstance(a.actual_value, str) else a.actual_value
            if a.prefix:
                if a.prefix.endswith(" "):
                    cmd.append(a.prefix[:-1])
                    cmd.append(v)
                    continue
                else:
                    v = f"{a.prefix}{v}"
            cmd.append(v)

        return_code = self._execute(cmd, mapped_std)
        if return_code not in (self.workflow.success_codes or {0}):
            raise errors.InvalidSuccessCode(return_code, success_codes=self.workflow.success_codes)

        return _upload_files(storage.writeGlobalFile, self.workflow.outputs, Path.cwd())


class SubprocessToilJob(AbstractToilJob):
    """A toil job that uses subprocess.call to execute a renku ``Plan``."""

    def __init__(self, workflow: Plan, *args, **kwargs):
        super(SubprocessToilJob, self).__init__(workflow, *args, **kwargs)

    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        return call(
            command_line,
            cwd=os.getcwd(),
            **{key: open(value, mode="r" if key == "stdin" else "w") for key, value in mapped_std.items()},
            env=self._environment,
        )


def _store_location(import_function: Callable[[str], FileID], basedir: Path, location: Path) -> Dict[str, Any]:
    if location.is_dir():
        directory_content = dict()
        for f in location.rglob("*"):
            directory_content[str(f.relative_to(basedir))] = _store_location(import_function, basedir, f)
        return directory_content
    else:
        return import_function(str(location))


def _upload_files(
    import_function: Callable[[str], FileID], params: List[CommandParameterBase], basedir: Path
) -> Dict[str, FileID]:
    file_locations = dict()
    for p in params:
        location = basedir / p.actual_value
        if not location.exists():
            continue

        file_locations[p.actual_value] = _store_location(import_function, basedir, location)

    return file_locations


def importFileWrapper(storage: AbstractFileStore, file_path: str) -> FileID:
    """Wrap importFile accept file:// URIs."""
    file_uri = file_path if ":/" in file_path else f"file://{file_path}"
    return storage.importFile(file_uri)


def process_children(parent: Job, dag: nx.DiGraph, jobs: Dict[str, Job], basedir: Path, storage: AbstractFileStore):
    """Recursively process children of a workflow."""

    outputs = list()
    import_function = functools.partial(importFileWrapper, storage)
    for child in nx.neighbors(dag, parent.workflow):
        child_job = jobs[id(child)]
        file_metadata = _upload_files(import_function, child.inputs, basedir)
        child_job.set_input_files(file_metadata)
        child_job.add_input_promise(parent.rv())
        outputs.append(parent.addFollowOn(child_job).rv())
        outputs += process_children(child_job, dag, jobs, basedir, storage)
    return outputs


def initialize_jobs(job, basedir, dag):
    """Creates the Toil execution plan for the given workflow DAG."""

    job.fileStore.logToMaster("executing renku DAG")
    outputs = list()
    jobs = {id(n): SubprocessToilJob(n) for n in dag.nodes}
    import_function = functools.partial(importFileWrapper, job.fileStore)
    children = next(nx.topological_generations(dag))
    for workflow in children:
        child_job = jobs[id(workflow)]
        file_metadata = _upload_files(import_function, workflow.inputs, basedir)
        child_job.set_input_files(file_metadata)
        outputs.append(job.addChild(child_job).rv())
        outputs += process_children(child_job, dag, jobs, basedir, job.fileStore)

    return outputs


class ToilProvider(IWorkflowProvider):
    """A workflow executor provider using toil."""

    @hookimpl
    def workflow_provider(self):
        """Workflow provider name."""
        return (self, "toil")

    @hookimpl
    def workflow_execute(self, dag: nx.DiGraph, basedir: Path, config: Dict[str, Any]):
        """Executes a given workflow DAG using Toil."""
        tmpdir = (Path(RENKU_HOME) / RENKU_TMP).resolve()
        if not tmpdir.exists():
            tmpdir.mkdir()
        options = Job.Runner.getDefaultOptions(str(tmpdir / uuid.uuid4().hex))
        options.logLevel = "ERROR"
        options.clean = "always"

        if config:
            [setattr(options, k, v) for k, v in config.items()]

        outputs = list()
        try:
            with Toil(options) as toil:
                rootJob = Job.wrapJobFn(initialize_jobs, basedir, dag)
                job_outputs = toil.start(rootJob)

                num_outputs = sum(map(lambda x: len(x.values()), job_outputs))
                with progressbar(length=num_outputs, label="Moving outputs") as bar:
                    for collection in job_outputs:
                        for name, fid in collection.items():
                            if isinstance(fid, dict):
                                directory = basedir / name
                                if directory.exists():
                                    shutil.rmtree(str(directory))
                                directory.mkdir()

                                for path, file_id in fid.items():
                                    toil.exportFile(file_id, str((basedir / path).absolute()))
                                    outputs.append(path)
                            else:
                                toil.exportFile(fid, str((basedir / name).absolute()))
                                outputs.append(name)
                            bar.update(1)
        except FailedJobsException as e:
            raise WorkflowExecuteError(e.msg)

        return outputs
