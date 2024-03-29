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
"""Represent a group of run templates."""

from marshmallow import EXCLUDE, pre_dump

from renku.command.schema.agent import PersonSchema
from renku.command.schema.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.command.schema.parameter import ParameterLinkSchema, ParameterMappingSchema
from renku.command.schema.plan import PlanSchema
from renku.domain_model.workflow.composite_plan import CompositePlan


class CompositePlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork, renku.CompositePlan]
        model = CompositePlan
        unknown = EXCLUDE

    description = fields.String(schema.description, load_default=None)
    id = fields.Id()
    creators = Nested(schema.creator, PersonSchema, many=True)
    mappings = Nested(renku.hasMappings, [ParameterMappingSchema], many=True, load_default=None)
    date_created = fields.DateTime(schema.dateCreated, format="iso")
    date_modified = fields.DateTime(schema.dateModified, format="iso")
    date_removed = fields.DateTime(prov.invalidatedAtTime, format="iso")
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)
    name = fields.String(schema.name, load_default=None)
    derived_from = fields.IRI(prov.wasDerivedFrom, load_default=None)
    project_id = fields.IRI(renku.hasPlan, reverse=True)
    plans = Nested(renku.hasSubprocess, [PlanSchema, "CompositePlanSchema"], many=True)
    links = Nested(renku.workflowLinks, [ParameterLinkSchema], many=True, load_default=None)

    @pre_dump(pass_many=True)
    def removes_ms(self, objs, many, **kwargs):
        """Remove milliseconds from datetimes.

        Note: since DateField uses `strftime` as format, which only supports timezone info without a colon
        e.g. `+0100` instead of `+01:00`, we have to deal with milliseconds manually instead of using a format string.
        """

        def _replace_times(obj):
            obj.unfreeze()
            obj.date_created = obj.date_created.replace(microsecond=0)
            obj.date_modified = obj.date_modified.replace(microsecond=0)
            if obj.date_removed:
                obj.date_removed = obj.date_removed.replace(microsecond=0)
            obj.freeze()

        if many:
            for obj in objs:
                _replace_times(obj)
            return objs

        _replace_times(objs)
        return objs
