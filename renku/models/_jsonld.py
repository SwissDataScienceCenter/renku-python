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

import json
from copy import deepcopy

import attr
from attr._compat import iteritems
from attr._funcs import has
from attr._make import Factory, fields
from pyld import jsonld as ld

from renku._compat import Path

KEY = '__json_ld'
KEY_CLS = '__json_ld_cls'

DOC_TPL = (
    "{cls.__doc__}\n\n"
    "**Type:**\n\n"
    ".. code-block:: json\n\n"
    "    \"{jsonld_cls._jsonld_type}\"\n\n"
    "**Context:**\n\n"
    ".. code-block:: json\n\n"
    "{context}\n"
)

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

            if KEY_CLS in a.metadata:
                merge_ctx = a.metadata[KEY_CLS]._jsonld_context
                for ctx_key, ctx_value in merge_ctx.items():
                    context.setdefault(ctx_key, ctx_value)
                    if context[ctx_key] != ctx_value:
                        raise TypeError(
                            'Can not merge {0} and {1} because of {2}'.format(
                                jsonld_cls, a.metadata[KEY_CLS], ctx_key
                            )
                        )

        jsonld_cls.__module__ = cls.__module__
        jsonld_cls._jsonld_type = type
        jsonld_cls._jsonld_context = context
        jsonld_cls._jsonld_translate = translate
        jsonld_cls._jsonld_fields = {a.name for a in attr.fields(jsonld_cls)}

        context_doc = '\n'.join(
            '   ' + line for line in json.dumps(context, indent=2).split('\n')
        )
        jsonld_cls.__doc__ = DOC_TPL.format(
            cls=cls, jsonld_cls=jsonld_cls, context=context_doc
        )
        return jsonld_cls

    if maybe_cls is None:
        return wrap
    return wrap(maybe_cls)


def attrib(context=None, **kwargs):
    """Create a new attribute with context."""
    kwargs.setdefault('metadata', {})
    if context:
        kwargs['metadata'][KEY] = context
    return attr.ib(**kwargs)


_container_types = (
    ('list', list, lambda type, value: [type.from_jsonld(v) for v in value]),
    ('set', set, lambda type, value: {type.from_jsonld(v)
                                      for v in value}),
    (
        'index', dict,
        lambda type, value: {k: type.from_jsonld(v)
                             for k, v in value.items()}
    ),
)


def _container_attrib_builder(name, container, mapper):
    """Builder for container attributes."""
    context = {'@container': '@{0}'.format(name)}

    def _attrib(type, **kwargs):
        """Define a container attribute."""
        kwargs.setdefault('metadata', {})
        kwargs['metadata'][KEY_CLS] = type
        kwargs['default'] = Factory(container)

        def _converter(value):
            """Convert value to the given type."""
            if isinstance(value, container):
                return mapper(type, value)
            elif value is None:
                return value
            raise ValueError(value)

        kwargs.setdefault('converter', _converter)
        return attrib(context=context, **kwargs)

    return _attrib


container = type(
    'Container', (object, ), {
        name: staticmethod(_container_attrib_builder(name, container, mapper))
        for name, container, mapper in _container_types
    }
)


def asjsonld(
    inst,
    recurse=True,
    filter=None,
    dict_factory=dict,
    retain_collection_types=False,
    export_context=True,
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
        # do not export context for containers
        ec = export_context and KEY_CLS not in a.metadata

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
                        dict_factory=dict_factory,
                        export_context=ec,
                    ) if has(i.__class__) else i for i in v
                ])
            elif isinstance(v, dict):
                df = dict_factory
                rv[a.name] = df((
                    asjsonld(kk, dict_factory=df) if has(kk.__class__) else kk,
                    asjsonld(vv, dict_factory=df, export_context=ec)
                    if has(vv.__class__) else vv
                ) for kk, vv in iteritems(v))
            else:
                rv[a.name] = convert_value(v)
        else:
            rv[a.name] = convert_value(v)

    inst_cls = type(inst)

    if export_context:
        rv['@context'] = deepcopy(inst_cls._jsonld_context)

    if inst_cls._jsonld_type:
        rv['@type'] = inst_cls._jsonld_type
    return rv


class JSONLDMixin(object):
    """Mixin for loading a JSON-LD data."""

    @classmethod
    def from_jsonld(cls, data):
        """Instantiate a JSON-LD class from data."""
        if isinstance(data, cls):
            return data

        if not isinstance(data, dict):
            raise ValueError(data)

        if cls._jsonld_translate:
            data = ld.compact(data, {'@context': cls._jsonld_translate})
            data.pop('@context', None)

        if '@context' in data and data['@context'] != cls._jsonld_context:
            compacted = ld.compact(data, cls._jsonld_context)
        else:
            compacted = data

        # assert compacted['@type'] == cls._jsonld_type, '@type must be equal'
        # TODO update self(not cls)._jsonld_context with data['@context']
        fields = cls._jsonld_fields
        return cls(**{k: v for k, v in compacted.items() if k in fields})


s = attrs
ib = attrib
