# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Represent workflows from the Common Workflow Language."""

import fnmatch
import uuid

import attr

from ._ascwl import CWLClass, mapped
from .parameter import WorkflowOutputParameter
from .process import Process
from .types import PATH_OBJECTS


@attr.s
class WorkflowStep(object):
    """Define an executable element of a workflow."""

    run = attr.ib()  # string, Process
    id = attr.ib(default=attr.Factory(uuid.uuid4))

    in_ = attr.ib(default=None)
    out = attr.ib(default=None)


@attr.s
class Workflow(Process, CWLClass):
    """Define a workflow representation."""

    outputs = mapped(WorkflowOutputParameter)
    steps = mapped(WorkflowStep)

    def add_step(self, **kwargs):
        """Add a workflow step."""
        self.steps.append(WorkflowStep(**kwargs))

    def get_output_id(self, path):  # pragma: no cover
        """Return an id of the matching path from default values."""
        for output in self.outputs:
            if output.type != 'File':
                continue
            if output.outputSource:
                step_id, _, source = output.outputSource.partition('/')
                for workflow_step in self.steps:
                    if workflow_step.id == step_id:
                        break
                else:
                    continue

                if source not in workflow_step.out:
                    continue

                # TODO load step and call get_output_id
            elif output.outputBinding:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in self.inputs:
                        if input_.id == input_id and input_.default == path:
                            return output.id
                elif fnmatch.fnmatch(path, glob):
                    return output.id

    def iter_output_files(self, basedir, commit=None, **kwargs):
        """Yield tuples with output id and path."""
        step_outputs = {}

        if commit:
            import yaml

            def _load(step):
                data = (commit.tree / basedir / step.run).data_stream.read()
                return CWLClass.from_cwl(yaml.load(data))

            tools = getattr(self, '_tools', None)
            if not tools:
                tools = {step.id: _load(step) for step in self.steps}

            for step in self.steps:
                step_outputs[step.id] = dict(
                    tools[step.id].iter_output_files(basedir, commit=commit)
                )

            setattr(self, '_tools', tools)
            setattr(self, '_step_outputs', step_outputs)

        for output in self.outputs:
            if output.type not in PATH_OBJECTS:
                continue

            if output.outputSource:
                step_id, _, source = output.outputSource.partition('/')
                glob = step_outputs.get(step_id, {}).get(source)
                if glob:
                    yield output.id, glob
            elif output.outputBinding:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in self.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                else:
                    yield output.id, glob
