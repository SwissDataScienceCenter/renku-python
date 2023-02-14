# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
from typing import Any, Callable, Dict, List, Union, cast

import networkx as nx
from toil.common import Toil
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job, Promise
from toil.leader import FailedJobsException
from toil.lib.docker import apiDockerCall

from renku.command.util import progressbar
from renku.core import errors
from renku.core.constant import RENKU_HOME, RENKU_TMP
from renku.core.errors import WorkflowExecuteError
from renku.core.plugin import hookimpl
from renku.core.plugin.provider import RENKU_ENV_PREFIX
from renku.domain_model.workflow.parameter import CommandParameterBase
from renku.domain_model.workflow.plan import Plan
from renku.domain_model.workflow.provider import IWorkflowProvider


class AbstractToilJob(Job):
    """Toil job implementation for a renku ``Plan``."""

    def __init__(self, workflow: Plan, *args, **kwargs):
        super(AbstractToilJob, self).__init__(unitName=workflow.name, displayName=workflow.name, *args, **kwargs)
        self.workflow: Plan = workflow
        self._input_files: Dict[str, FileID] = {}
        self._parents_promise: List[Promise] = []
        self._environment = os.environ.copy()

    @abstractmethod
    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        raise NotImplementedError

    def set_input_files(self, input_files: Dict[str, Any]):
        """Set the location of inputs that are available in the original project."""
        self._input_files = input_files

    def add_input_promise(self, promise: Dict[str, FileID]):
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

            self._environment[f"{RENKU_ENV_PREFIX}{i.name}"] = str(i.actual_value)

            if i.mapped_to:
                mapped_std[i.mapped_to.stream_type] = i.actual_value

        for o in self.workflow.outputs:
            self._environment[f"{RENKU_ENV_PREFIX}{o.name}"] = str(o.actual_value)
            output_path = Path(o.actual_value)
            if len(output_path.parts) > 1:
                output_path.parent.mkdir(parents=True, exist_ok=True)

            if o.mapped_to:
                mapped_std[o.mapped_to.stream_type] = o.actual_value

        for p in self.workflow.parameters:
            self._environment[f"{RENKU_ENV_PREFIX}{p.name}"] = str(p.actual_value)

        # construct cmd
        cmd = []

        if self.workflow.command:
            cmd.extend(self.workflow.command.split(" "))

        arguments = itertools.chain(self.workflow.inputs, self.workflow.outputs, self.workflow.parameters)

        arguments_filtered = filter(lambda x: x.position is not None and not getattr(x, "mapped_to", None), arguments)
        arguments_sorted = sorted(arguments_filtered, key=lambda x: cast(int, x.position))

        for a in arguments_sorted:
            v = str(a.actual_value) if not isinstance(a.actual_value, str) else a.actual_value
            if a.prefix:
                if a.prefix.endswith(" "):
                    cmd.append(a.prefix[:-1])
                else:
                    v = f"{a.prefix}{v}"
            cmd.append(v)

        return_code = self._execute(cmd, mapped_std)
        if return_code not in (self.workflow.success_codes or {0}):
            raise errors.InvalidSuccessCode(return_code, success_codes=self.workflow.success_codes)

        return _upload_files(
            storage.writeGlobalFile, cast(List[CommandParameterBase], self.workflow.outputs), Path.cwd()
        )


class SubprocessToilJob(AbstractToilJob):
    """A toil job that uses subprocess.call to execute a renku ``Plan``."""

    def __init__(self, workflow: Plan, *args, **kwargs):
        super().__init__(workflow, *args, **kwargs)

    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        return call(
            command_line,
            cwd=os.getcwd(),
            **{  # type: ignore
                key: open(value, mode="r" if key == "stdin" else "w")
                for key, value in mapped_std.items()  # type: ignore
            },
            env=self._environment,
        )


class DockerToilJob(AbstractToilJob):
    """A toil job that uses apiDockerCall to execute a renku ``Plan``."""

    def __init__(self, workflow: Plan, docker_config: Dict[str, Any], *args, **kwargs):
        super().__init__(workflow, *args, **kwargs)
        self._docker_config: Dict[str, Any] = docker_config

    def _execute(self, command_line: List[str], mapped_std: Dict[str, str]) -> int:
        """Executes a given command line."""
        # NOTE: Disable detached mode to block for ``apiDockerCall`` to finish
        self._docker_config.pop("detach", None)

        parameters: Union[List[str], List[List[str]]] = command_line

        stderr = mapped_std.get("stderr")
        stdin = mapped_std.get("stdin")
        if stderr or stdin:
            raise errors.OperationError("Cannot run workflows that have stdin or stderr redirection with Docker")
        stdout = mapped_std.get("stdout")
        if stdout:
            parameters = [command_line, ["tee", stdout]]

        volumes = self._docker_config.pop("volumes", {})

        if "working_dir" not in self._docker_config:
            working_dir = "/renku"
            volumes[os.getcwd()] = {"bind": working_dir, "mode": "rw"}
        else:
            working_dir = self._docker_config.pop("working_dir")

        # NOTE: We cannot get the exit code back from the docker container. The Docker API checks for the exit code and
        # raises an exception if it's not 0, so, Plan.success_codes is ignored when running with Docker.
        apiDockerCall(
            self,
            parameters=parameters,
            detach=False,
            environment=self._environment,
            volumes=volumes,
            working_dir=working_dir,
            **self._docker_config,
        )

        return 0


def _store_location(
    import_function: Callable[[str], FileID], basedir: Path, location: Path
) -> Union[FileID, Dict[str, Any]]:
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


def import_file_wrapper(storage: AbstractFileStore, file_path: str) -> FileID:
    """Wrap importFile accept file:// URIs."""
    file_uri = file_path if ":/" in file_path else f"file://{file_path}"
    return storage.importFile(file_uri)


def process_children(
    parent: AbstractToilJob,
    dag: nx.DiGraph,
    jobs: Dict[int, AbstractToilJob],
    basedir: Path,
    storage: AbstractFileStore,
):
    """Recursively process children of a workflow."""
    outputs = list()
    import_function = functools.partial(import_file_wrapper, storage)
    for child in nx.neighbors(dag, parent.workflow):
        child_job = jobs[id(child)]
        file_metadata = _upload_files(import_function, child.inputs, basedir)
        child_job.set_input_files(file_metadata)
        child_job.add_input_promise(parent.rv())
        outputs.append(parent.addFollowOn(child_job).rv())
        outputs += process_children(child_job, dag, jobs, basedir, storage)
    return outputs


def initialize_jobs(job, basedir, dag, docker_config):
    """Creates the Toil execution plan for the given workflow DAG."""
    job.fileStore.logToMaster("executing renku DAG")
    outputs = list()
    if docker_config:
        job.fileStore.logToMaster("executing with Docker")
        jobs = {id(n): DockerToilJob(n, docker_config) for n in dag.nodes}
    else:
        jobs = {id(n): SubprocessToilJob(n) for n in dag.nodes}
    import_function = functools.partial(import_file_wrapper, job.fileStore)
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
        return self, "toil"

    @hookimpl
    def workflow_execute(self, dag: nx.DiGraph, basedir: Path, config: Dict[str, Any]):
        """Executes a given workflow DAG using Toil."""
        tmpdir = (basedir / RENKU_HOME / RENKU_TMP).resolve()
        if not tmpdir.exists():
            tmpdir.mkdir()
        options = Job.Runner.getDefaultOptions(str(tmpdir / uuid.uuid4().hex))
        options.logLevel = "ERROR"
        options.clean = "always"

        config = config or {}
        docker_config = config.pop("docker", {})
        if docker_config and "image" not in docker_config:
            raise errors.ConfigurationError("Docker configuration must provide an 'image' property")

        if config:
            for k, v in config.items():
                setattr(options, k, v)

        outputs = list()
        try:
            with Toil(options) as toil:
                root_job = Job.wrapJobFn(initialize_jobs, basedir, dag, docker_config)
                job_outputs = toil.start(root_job)

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
                                    destination = (basedir / path).absolute()
                                    destination.parent.mkdir(parents=True, exist_ok=True)
                                    toil.exportFile(file_id, str(destination))
                                    outputs.append(path)
                            else:
                                destination = (basedir / name).absolute()
                                destination.parent.mkdir(parents=True, exist_ok=True)
                                toil.exportFile(fid, str(destination))
                                outputs.append(name)
                            bar.update(1)
        except FailedJobsException as e:
            raise WorkflowExecuteError(e.msg)

        return outputs
