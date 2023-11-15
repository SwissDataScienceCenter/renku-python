# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Represent workflow file run templates."""

from itertools import chain
from typing import List, Union

import marshmallow

from renku.command.schema.calamus import fields, prov, renku, schema
from renku.command.schema.composite_plan import CompositePlanSchema
from renku.command.schema.plan import PlanSchema
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan, WorkflowFilePlan


class WorkflowFilePlanSchema(PlanSchema):
    """WorkflowFilePlan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork, renku.Plan, renku.WorkflowFilePlan]
        model = WorkflowFilePlan
        unknown = marshmallow.EXCLUDE

    @marshmallow.pre_dump(pass_many=True)
    def fix_ids(self, objs: Union[WorkflowFilePlan, List[WorkflowFilePlan]], many, **kwargs):
        """Renku up to 2.4.1 had a bug that created wrong ids for workflow file entities, this fixes those on export."""

        def _replace_id(obj):
            obj.unfreeze()
            obj.id = obj.id.replace("//plans/", "/")

            for child in chain(obj.inputs, obj.outputs, obj.parameters):
                child.id = child.id.replace("//plans/", "/")
            obj.freeze()

        if many:
            for obj in objs:
                _replace_id(obj)
            return objs

        _replace_id(objs)
        return objs


class WorkflowFileCompositePlanSchema(CompositePlanSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork, renku.CompositePlan, renku.WorkflowFileCompositePlan]
        model = WorkflowFileCompositePlan
        unknown = marshmallow.EXCLUDE

    path = fields.String(prov.atLocation)
    plans = fields.Nested(renku.hasSubprocess, WorkflowFilePlanSchema, many=True)

    @marshmallow.pre_dump(pass_many=True)
    def fix_ids(self, objs, many, **kwargs):
        """Renku up to 2.4.1 had a bug that created wrong ids for workflow file entities, this fixes those on export."""

        def _replace_id(obj):
            obj.unfreeze()
            obj.id = obj.id.replace("//plans/", "/")
            obj.freeze()

        if many:
            for obj in objs:
                _replace_id(obj)
            return objs

        _replace_id(objs)
        return objs
