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
"""Graph builder."""

import attr

from renku.models import _jsonld as jsonld


@jsonld.s(
    type='prov:Entity',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
        'dcterms': 'http://purl.org/dc/terms/',
    },
)
class Entity(object):
    """An entity is a thing with some fixed aspects."""

    title = jsonld.ib(context='dcterms:title', default=None)


@jsonld.s(
    type='prov:Usage',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
)
class Usage(object):
    """Usage is the beginning of utilizing an entity by an activity."""

    entity = jsonld.ib(context='prov:entity')
    had_role = jsonld.ib(context='prov:hadRole', default=None)
    at_time = jsonld.ib(
        context={
            '@id': 'prov:atTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        default=None
    )


@jsonld.s(
    type='prov:Activity',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
)
class Activity(object):
    """An activity occurs acts upon or with entities over period of time."""

    used = jsonld.ib(context='prov:used')
    generated = jsonld.ib(context='prov:generated', default=attr.Factory(list))
    started_at_time = jsonld.ib(
        context={
            '@id': 'prov:startedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        default=None
    )

    ended_at_time = jsonld.ib(
        context={
            '@id': 'prov:endedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        default=None
    )
