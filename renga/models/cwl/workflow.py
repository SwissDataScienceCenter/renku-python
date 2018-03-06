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

import uuid

import attr

from ._ascwl import CWLClass, mapped
from .parameter import WorkflowOutputParameter
from .process import Process


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
