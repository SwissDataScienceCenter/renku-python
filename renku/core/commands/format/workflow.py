# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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


def tabular(workflows, *, columns=None):
    """Format workflows with a tabular output."""
    if not columns:
        columns = "id,name,keywords"

    _create_workflow_short_description(workflows)

    return tabulate(collection=workflows, columns=columns, columns_mapping=WORKFLOW_COLUMNS)


def _create_workflow_short_description(workflows):
    for workflow in workflows:
        lines = textwrap.wrap(workflow.description, width=64, max_lines=5) if workflow.description else []
        workflow.short_description = "\n".join(lines)


def jsonld(workflows, **kwargs):
    """Format workflow as JSON-LD."""
    from renku.core.commands.schema.plan import PlanSchema

    data = [PlanSchema().dump(workflow) for workflow in workflows]
    return dumps(data, indent=2)


def json(workflows, **kwargs):
    """Format workflow as JSON."""
    from renku.core.models.workflow.plan import PlanDetailsJson

    data = [PlanDetailsJson().dump(workflow) for workflow in workflows]
    return dumps(data, indent=2)


WORKFLOW_FORMATS = {
    "tabular": tabular,
    "json-ld": jsonld,
    "json": json,
}
"""Valid formatting options."""

WORKFLOW_COLUMNS = {
    "id": ("id", "id"),
    "name": ("name", None),
    "keywords": ("keywords_csv", "keywords"),
    "description": ("short_description", "description"),
    "command": ("full_command", "command"),
}
