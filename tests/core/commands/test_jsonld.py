# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Test JSON-LD definitions."""

from renku.core.models import jsonld as jsonld


def test_inheritance():
    """Test type and context inheritance."""
    types = {'prov:Activity', 'wfprov:ProcessRun'}
    context_keys = {'prov', 'wfprov', '@version'}

    @jsonld.s(type='prov:Activity', context={'prov': 'A'})
    class Activity:
        """Define an activity."""

    @jsonld.s(type='wfprov:ProcessRun', context={'wfprov': 'B'})
    class ProcessRun(Activity):
        """Define a process execution based on an activity."""

    data = jsonld.asjsonld(ProcessRun())
    assert types == set(data['@type'])
    assert context_keys == set(data['@context'].keys())

    types = {'prov:Activity', 'wfprov:ProcessRun', 'wfprov:WorkflowRun'}

    @jsonld.s(type='wfprov:WorkflowRun')
    class WorkflowRun(ProcessRun):
        """Define a workflow run."""

    data = jsonld.asjsonld(WorkflowRun())
    assert types == set(data['@type'])
    assert context_keys == set(data['@context'].keys())
