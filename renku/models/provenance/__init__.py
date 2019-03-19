# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Define classes used for capturing data provenance.

.. seealso:: https://www.w3.org/TR/prov-o/
"""

from .activities import Activity, ProcessRun, WorkflowRun
from .agents import Person, SoftwareAgent
from .entities import Collection, Entity, Process, Workflow
from .expanded import Project
from .qualified import Generation, Usage

__all__ = (
    'Activity',
    'Entity',
    'Collection',
    'Generation',
    'Person',
    'Process',
    'ProcessRun',
    'Project',
    'SoftwareAgent',
    'Usage',
    'Workflow',
    'WorkflowRun',
)
