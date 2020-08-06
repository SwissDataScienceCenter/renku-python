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
"""Represent provenance processes."""

import weakref

from renku.core.models import jsonld as jsonld
from renku.core.models.entities import CommitMixin


@jsonld.s(
    type=["wfdesc:Process", "prov:Entity", "prov:Plan",],
    context={"wfdesc": "http://purl.org/wf4ever/wfdesc#", "prov": "http://www.w3.org/ns/prov#",},
    cmp=False,
)
class Process(CommitMixin):
    """Represent a process."""

    _activity = jsonld.ib(
        default=None,
        context="prov:activity",
        kw_only=True,
        converter=lambda value: weakref.ref(value) if value is not None else None,
        type="renku.core.models.provenance.activities.Activity",
    )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity()


@jsonld.s(
    type=["wfdesc:Workflow", "prov:Entity", "prov:Plan",],
    context={"wfdesc": "http://purl.org/wf4ever/wfdesc#", "prov": "http://www.w3.org/ns/prov#",},
    cmp=False,
)
class Workflow(Process):
    """Represent workflow with subprocesses."""

    subprocesses = jsonld.ib(context="wfdesc:hasSubProcess", kw_only=True)

    @subprocesses.default
    def default_subprocesses(self):
        """Load subprocesses."""
        return [subprocess.association.plan for subprocess in self.activity.subprocesses.values()]
