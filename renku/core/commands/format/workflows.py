# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Serializers for workflows."""
import textwrap

from renku.core.models.json import dumps

from .tabulate import tabulate


def tabular(client, workflows, *, columns=None):
    """Format workflows with a tabular output."""
    if not columns:
        columns = "id,name,description"

    _create_workflow_short_description(workflows)

    return tabulate(collection=workflows, columns=columns, columns_mapping=WORKFLOWS_COLUMNS)


def _create_workflow_short_description(workflows):
    for workflow in workflows:
        lines = textwrap.wrap(workflow.description, width=64, max_lines=5) if workflow.description else []
        workflow.short_description = "\n".join(lines)


def jsonld(client, workflows, **kwargs):
    """Format workflows as JSON-LD."""
    data = [workflow.as_jsonld() for workflow in workflows]
    return dumps(data, indent=2)


WORKFLOWS_FORMATS = {
    "tabular": tabular,
    "json-ld": jsonld,
}
"""Valid formatting options."""

WORKFLOWS_COLUMNS = {
    "id": ("identifier", "id"),
    "name": ("name", None),
    "description": ("short_description", "description"),
}
