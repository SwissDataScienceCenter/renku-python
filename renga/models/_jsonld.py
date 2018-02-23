# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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
"""Support JSON-LD context in models."""

import datetime
from copy import deepcopy

import attr
from pyld import jsonld as ld

KEY = '__json_ld'


def attrs(maybe_cls=None, type=None, context=None, translate=None):
    """Wrap an attr enabled class."""
    context = context or {}
    translate = translate or {}

    def wrap(cls):
        """Decorate an attr enabled class."""
        cls._jsonld_type = type
        jsonld_cls = attr.s(cls, slots=True)

        for a in attr.fields(jsonld_cls):
            try:
                key = a.name
                ctx = a.metadata[KEY]
            except KeyError:
                continue

            if ':' in ctx:
                prefix, _ = ctx.split(':', 1)
                if prefix in context:
                    context[key] = ctx
                    continue

            if ctx not in context:
                context[key] = ctx

        jsonld_cls._jsonld_context = context
        jsonld_cls._jsonld_translate = translate
        return jsonld_cls

    if maybe_cls is None:
        return wrap
    return wrap(maybe_cls)


def attrib(context=None, **kwargs):
    """Create a new attribute with context."""
    if context:
        kwargs.setdefault('metadata', {})
        kwargs['metadata'][KEY] = context
    return attr.ib(**kwargs)


def asjsonld(jsonld_obj, *args, **kwargs):
    """Dump a JSON-LD class to the JSON with generated ``@context`` field."""
    data = attr.asdict(jsonld_obj, *args, **kwargs)
    data['@context'] = context = deepcopy(jsonld_obj.__class__._jsonld_context)
    if jsonld_obj.__class__._jsonld_type:
        data['@type'] = jsonld_obj.__class__._jsonld_type
    return data


class JSONLDMixin(object):
    """Mixin for loading a JSON-LD data."""

    @classmethod
    def from_jsonld(cls, data):
        """Instantiate a JSON-LD class from data."""
        if cls._jsonld_translate:
            data = ld.compact(data, {'@context': cls._jsonld_translate})
            data.pop('@context', None)

        compacted = ld.compact(data, cls._jsonld_context)
        # assert compacted['@type'] == cls._jsonld_type, '@type must be equal'
        # TODO update self(not cls)._jsonld_context with data['@context']
        return cls(**{k: v for k, v in compacted.items() if hasattr(cls, k)})


s = attrs
ib = attrib
