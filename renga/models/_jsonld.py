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

from copy import deepcopy

import attr
from attr._compat import iteritems
from attr._funcs import has
from attr._make import fields
from pyld import jsonld as ld

from renga._compat import Path

KEY = '__json_ld'

make_type = type


def attrs(
    maybe_cls=None, type=None, context=None, translate=None, **attrs_kwargs
):
    """Wrap an attr enabled class."""
    context = context or {}
    translate = translate or {}

    def wrap(cls):
        """Decorate an attr enabled class."""
        jsonld_cls = attr.s(cls, **attrs_kwargs)

        if not issubclass(jsonld_cls, JSONLDMixin):
            jsonld_cls = make_type(cls.__name__, (jsonld_cls, JSONLDMixin), {})

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

            if isinstance(ctx, dict) or ctx not in context:
                context[key] = ctx

        jsonld_cls._jsonld_type = type
        jsonld_cls._jsonld_context = context
        jsonld_cls._jsonld_translate = translate
        jsonld_cls._jsonld_fields = {a.name for a in attr.fields(jsonld_cls)}
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


def asjsonld(
    inst,
    recurse=True,
    filter=None,
    dict_factory=dict,
    retain_collection_types=False
):
    """Dump a JSON-LD class to the JSON with generated ``@context`` field."""
    attrs = fields(inst.__class__)
    rv = dict_factory()

    def convert_value(v):
        """Convert special types."""
        if isinstance(v, Path):
            return str(v)
        return v

    for a in attrs:
        v = getattr(inst, a.name)
        if filter is not None and not filter(a, v):
            continue
        if recurse is True:
            if has(v.__class__):
                rv[a.name] = asjsonld(
                    v, recurse=True, filter=filter, dict_factory=dict_factory
                )
            elif isinstance(v, (tuple, list, set)):
                cf = v.__class__ if retain_collection_types is True else list
                rv[a.name] = cf([
                    asjsonld(
                        i,
                        recurse=True,
                        filter=filter,
                        dict_factory=dict_factory
                    ) if has(i.__class__) else i for i in v
                ])
            elif isinstance(v, dict):
                df = dict_factory
                rv[a.name] = df((
                    asjsonld(kk, dict_factory=df) if has(kk.__class__) else kk,
                    asjsonld(vv, dict_factory=df) if has(vv.__class__) else vv
                ) for kk, vv in iteritems(v))
            else:
                rv[a.name] = convert_value(v)
        else:
            rv[a.name] = convert_value(v)

    inst_cls = type(inst)
    rv['@context'] = deepcopy(inst_cls._jsonld_context)

    if inst_cls._jsonld_type:
        rv['@type'] = inst_cls._jsonld_type
    return rv


class JSONLDMixin(object):
    """Mixin for loading a JSON-LD data."""

    @classmethod
    def from_jsonld(cls, data):
        """Instantiate a JSON-LD class from data."""
        if cls._jsonld_translate:
            data = ld.compact(data, {'@context': cls._jsonld_translate})
            data.pop('@context', None)

        # FIXME compacted = ld.compact(data, cls._jsonld_context)
        compacted = data
        # assert compacted['@type'] == cls._jsonld_type, '@type must be equal'
        # TODO update self(not cls)._jsonld_context with data['@context']
        fields = cls._jsonld_fields
        return cls(**{k: v for k, v in compacted.items() if k in fields})


s = attrs
ib = attrib
