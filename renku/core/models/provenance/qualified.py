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
"""Represent elaborated information about relations."""

import weakref

import attr
from marshmallow import EXCLUDE

from renku.core.models import jsonld as jsonld
from renku.core.models.calamus import JsonLDSchema, fields, prov
from renku.core.models.datasets import Dataset, DatasetFile, \
    DatasetFileSchema, DatasetSchema
from renku.core.models.entities import CollectionSchema, EntitySchema
from renku.core.models.provenance.agents import PersonSchema, \
    SoftwareAgentSchema
from renku.core.models.workflow.run import RunSchema


@jsonld.s(
    type='prov:Association',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
)
class Association:
    """Assign responsibility to an agent for an activity."""

    plan = jsonld.ib(
        context='prov:hadPlan', type='renku.core.models.workflow.run.Run'
    )
    agent = jsonld.ib(
        context='prov:agent',
        default=None,
        type='renku.core.models.provenance.agents.SoftwareAgent'
    )

    _id = jsonld.ib(context='@id', kw_only=True)

    @classmethod
    def from_activity(cls, activity, commit=None):
        """Create an instance from the activity."""
        from .agents import SoftwareAgent

        agent = SoftwareAgent.from_commit(activity.commit)
        return cls(
            plan=activity.__association_cls__(
                commit=commit or activity.commit,
                client=activity.client,
                path=activity.path,
                activity=activity,
            ),
            agent=agent,
            id=activity._id + '/association',  # add plan and agent
        )

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return AssociationSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return AssociationSchema().dump(self)


class EntityProxyMixin:
    """Implement proxy to entity attribute."""

    def __getattribute__(self, name):
        """Proxy entity attributes."""
        cls = object.__getattribute__(self, '__class__')
        names = {field.name for field in attr.fields(cls)}
        names |= set(dir(cls))
        if name in names:
            return object.__getattribute__(self, name)
        entity = object.__getattribute__(self, 'entity')
        return getattr(entity, name)


@jsonld.s(
    type='prov:Usage',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Usage(EntityProxyMixin):
    """Represent a dependent path."""

    entity = jsonld.ib(
        context='prov:entity',
        kw_only=True,
        type=[
            'renku.core.models.entities.Entity',
            'renku.core.models.entities.Collection', Dataset, DatasetFile
        ]
    )
    role = jsonld.ib(context='prov:hadRole', default=None, kw_only=True)

    _id = jsonld.ib(context='@id', default=None, kw_only=True)

    @classmethod
    def from_revision(cls, client, path, revision='HEAD', **kwargs):
        """Return dependency from given path and revision."""
        from renku.core.models.entities import Entity

        return cls(
            entity=Entity.from_revision(client, path, revision), **kwargs
        )

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return UsageSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return UsageSchema().dump(self)


@jsonld.s(
    type='prov:Generation',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Generation(EntityProxyMixin):
    """Represent an act of generating a file."""

    entity = jsonld.ib(
        context={
            '@reverse': 'prov:qualifiedGeneration',
        },
        type=[
            'renku.core.models.entities.Entity',
            'renku.core.models.entities.Collection', Dataset, DatasetFile
        ]
    )

    role = jsonld.ib(context='prov:hadRole', default=None)

    _activity = attr.ib(
        default=None,
        kw_only=True,
        converter=lambda value: weakref.ref(value)
        if value is not None else None,
    )
    _id = jsonld.ib(context='@id', kw_only=True)

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity() if self._activity is not None else None

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        if self.role:
            return '{self.activity._id}/outputs/{self.role}'.format(
                self=self,
            )
        return '{self.activity._id}/tree/{self.entity.path}'.format(
            self=self,
        )

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return GenerationSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return GenerationSchema().dump(self)


class AssociationSchema(JsonLDSchema):
    """Association schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Association
        model = Association
        unknown = EXCLUDE

    _id = fields.Id(init_name='id')
    plan = fields.Nested(prov.hadPlan, RunSchema)
    agent = fields.Nested(prov.agent, [SoftwareAgentSchema, PersonSchema])


class UsageSchema(JsonLDSchema):
    """Usage schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Usage
        model = Usage
        unknown = EXCLUDE

    _id = fields.Id(init_name='id')
    entity = fields.Nested(
        prov.entity,
        [EntitySchema, CollectionSchema, DatasetSchema, DatasetFileSchema]
    )
    role = fields.String(prov.hadRole)


class GenerationSchema(JsonLDSchema):
    """Generation schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Generation
        model = Generation
        unknown = EXCLUDE

    _id = fields.Id(init_name='id')
    entity = fields.Nested(
        prov.qualifiedGeneration,
        [EntitySchema, CollectionSchema, DatasetSchema, DatasetFileSchema],
        reverse=True
    )
    role = fields.String(prov.hadRole)
