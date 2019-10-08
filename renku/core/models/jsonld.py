# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
import os
import weakref
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import attr
import yaml
from attr._compat import iteritems
from attr._funcs import has
from attr._make import Factory, fields
from pyld import jsonld as ld

from renku.core.models.locals import ReferenceMixin, with_reference
from renku.core.models.migrations import JSONLD_MIGRATIONS

KEY = '__json_ld'
KEY_CLS = '__json_ld_cls'

DOC_TPL = (
    '{cls.__doc__}\n\n'
    '**Type:**\n\n'
    '.. code-block:: json\n\n'
    '    {type}\n\n'
    '**Context:**\n\n'
    '.. code-block:: json\n\n'
    '{context}\n'
)

make_type = type


# Shamelessly copy/pasting from SO:
# https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
# This is needed to allow us to load from yaml and use json down the line.
class NoDatesSafeLoader(yaml.SafeLoader):
    """Used to safely load basic python objects but ignore datetime strings."""

    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Remove implicit resolvers for a particular tag.

        Takes care not to modify resolvers in super classes.

        We want to load datetimes as strings, not dates, because we
        go on to serialise as json which doesn't have the advanced types
        of yaml, and leads to incompatibilities down the track.
        """
        if 'yaml_implicit_resolvers' not in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = [
                (tag, regexp)
                for tag, regexp in mappings if tag != tag_to_remove
            ]


NoDatesSafeLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


def attrs(
    maybe_cls=None, type=None, context=None, translate=None, **attrs_kwargs
):
    """Wrap an attr enabled class."""
    if isinstance(type, (list, tuple, set)):
        types = list(type)
    else:
        types = [type] if type is not None else []
    context = context or {}
    translate = translate or {}

    def wrap(cls):
        """Decorate an attr enabled class."""
        jsonld_cls = attr.s(cls, **attrs_kwargs)

        if not issubclass(jsonld_cls, JSONLDMixin):
            jsonld_cls = attr.s(
                make_type(cls.__name__, (jsonld_cls, JSONLDMixin), {}),
                **attrs_kwargs
            )

        # Merge types
        for subcls in jsonld_cls.mro():
            subtype = getattr(subcls, '_jsonld_type', None)
            if subtype:
                if isinstance(subtype, (tuple, list)):
                    types.extend(subtype)
                else:
                    types.append(subtype)

            for key, value in getattr(subcls, '_jsonld_context', {}).items():
                if key in context and context[key] != value:
                    raise TypeError()
                context.setdefault(key, value)

        for a in attr.fields(jsonld_cls):
            key = a.name
            ctx = a.metadata.get(KEY)
            if ctx is None:
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
        jsonld_cls._jsonld_type = types[0] if len(types) == 1 else list(
            sorted(set(types))
        )
        jsonld_cls._jsonld_context = context
        jsonld_cls._jsonld_translate = translate
        jsonld_cls._jsonld_fields = {
            a.name
            for a in attr.fields(jsonld_cls) if KEY in a.metadata
        }

        context_doc = '\n'.join(
            '   ' + line for line in json.dumps(context, indent=2).split('\n')
        )
        jsonld_cls.__doc__ = DOC_TPL.format(
            cls=cls,
            type=json.dumps(jsonld_cls._jsonld_type),
            context=context_doc,
        )

        # Register class for given JSON-LD @type
        try:
            type_ = ld.expand({
                '@type': jsonld_cls._jsonld_type,
                '@context': context
            })[0]['@type']
            if isinstance(type_, list):
                type_ = tuple(sorted(type_))
        except Exception:
            # FIXME make sure all classes have @id defined
            return jsonld_cls

        if type_ in jsonld_cls.__type_registry__:
            raise TypeError(
                'Type {0!r} is already registered for class {1!r}.'.format(
                    jsonld_cls._jsonld_type,
                    jsonld_cls.__type_registry__[jsonld_cls._jsonld_type],
                )
            )
        jsonld_cls.__type_registry__[type_] = jsonld_cls
        return jsonld_cls

    if maybe_cls is None:
        return wrap
    return wrap(maybe_cls)


def attrib(context=None, **kwargs):
    """Create a new attribute with context."""
    kwargs.setdefault('metadata', {})
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
    factory = Factory(container)

    def _attrib(type, **kwargs):
        """Define a container attribute."""
        kwargs.setdefault('metadata', {})
        kwargs['metadata'][KEY_CLS] = type
        kwargs['default'] = factory

        def _converter(value):
            """Convert value to the given type."""
            if isinstance(value, container):
                return mapper(type, value)
            elif value is None:
                return value

            raise ValueError(value)

        kwargs.setdefault('converter', _converter)

        return attrib(**kwargs)

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
    basedir=None,
):
    """Dump a JSON-LD class to the JSON with generated ``@context`` field."""
    jsonld_fields = inst.__class__._jsonld_fields
    attrs = tuple(
        field
        for field in fields(inst.__class__) if field.name in jsonld_fields
    )
    rv = dict_factory()

    def convert_value(value):
        """Convert non-serializable types."""
        if isinstance(value, Path):
            result = str(value)
            if basedir:
                result = os.path.relpath(result, str(basedir))
            return result

        if isinstance(value, datetime):
            if not value.tzinfo:
                # set timezone to local timezone
                tz = datetime.now(timezone.utc).astimezone().tzinfo
                value = value.replace(tzinfo=tz)
            return value.isoformat()

        return value

    for a in attrs:
        v = getattr(inst, a.name)

        # skip proxies
        if isinstance(v, weakref.ReferenceType):
            continue

        # do not export context for containers
        ec = export_context and KEY_CLS not in a.metadata

        if filter is not None and not filter(a, v):
            continue
        if recurse is True:
            if has(v.__class__):
                rv[a.name] = asjsonld(
                    v,
                    recurse=True,
                    filter=filter,
                    dict_factory=dict_factory,
                    basedir=basedir,
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
                        basedir=basedir,
                    ) if has(i.__class__) else i for i in v
                ])
            elif isinstance(v, dict):
                df = dict_factory
                rv[a.name] = df((
                    asjsonld(
                        kk,
                        dict_factory=df,
                        basedir=basedir,
                    ) if has(kk.__class__) else convert_value(kk),
                    asjsonld(
                        vv,
                        dict_factory=df,
                        export_context=ec,
                        basedir=basedir,
                    ) if has(vv.__class__) else vv
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


class JSONLDMixin(ReferenceMixin):
    """Mixin for loading a JSON-LD data."""

    __type_registry__ = {}

    @classmethod
    def from_jsonld(
        cls,
        data,
        client=None,
        commit=None,
        __reference__=None,
        __source__=None,
    ):
        """Instantiate a JSON-LD class from data."""
        if isinstance(data, cls):
            return data

        if not isinstance(data, dict):
            raise ValueError(data)

        if '@type' in data:
            # @type could be a string or a list - make sure it is a list
            type_ = data['@type']
            if not isinstance(type_, list):
                type_ = [type_]
            # If a json-ld class has multiple types, they are in a
            # sorted tuple. This is used as the key for the class
            # registry, so we have to match it here.
            type_ = tuple(sorted(type_))
            if type_ in cls.__type_registry__ and getattr(
                cls, '_jsonld_type', None
            ) != type_:
                new_cls = cls.__type_registry__[type_]
                if cls != new_cls:
                    return new_cls.from_jsonld(
                        data, client=client, commit=commit
                    )

        if cls._jsonld_translate:
            # perform the translation
            data = ld.compact(data, cls._jsonld_translate)
            # compact using the class json-ld context
            data.pop('@context', None)
            data = ld.compact(data, cls._jsonld_context)

        data.setdefault('@context', cls._jsonld_context)

        schema_type = data.get('@type')
        migrations = []

        if isinstance(schema_type, list):
            for schema in schema_type:
                mig_ = JSONLD_MIGRATIONS.get(schema)
                if mig_:
                    migrations += mig_

        if isinstance(schema_type, str) and not migrations:
            migrations += JSONLD_MIGRATIONS.get(schema_type, [])

        for migration in set(migrations):
            data = migration(data)
            if __source__:
                __source__ = migration(__source__)

        if data['@context'] != cls._jsonld_context:
            try:
                compacted = ld.compact(data, cls._jsonld_context)
            except Exception:
                compacted = data
        else:
            compacted = data

        fields = cls._jsonld_fields

        data_ = {}
        # `client` and `commit` are passed in optionally for some classes
        # They might be unset if the metadata is used to instantiate
        # an object outside of a repo/client context.
        if client:
            data_['client'] = client
        if commit:
            data_['commit'] = commit

        for k, v in compacted.items():
            if k in fields:
                data_[k.lstrip('_')] = v

        if __reference__:
            with with_reference(__reference__):
                self = cls(**data_)
        else:
            self = cls(**data_)

        if __source__:
            setattr(self, '__source__', __source__)

        return self

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Return an instance from a YAML file."""
        import yaml

        with path.open(mode='r') as fp:
            source = yaml.load(fp, Loader=NoDatesSafeLoader) or {}
            self = cls.from_jsonld(
                source,
                client=client,
                commit=commit,
                __reference__=path,
                __source__=deepcopy(source)
            )
        return self

    def asjsonld(self):
        """Create JSON-LD with the original source data."""
        source = {}
        if self.__source__:
            source.update(self.__source__)
        source.update(asjsonld(self))
        return source

    def to_yaml(self):
        """Store an instance to the referenced YAML file."""
        dumper = yaml.dumper.Dumper
        dumper.ignore_aliases = lambda _, data: True

        with self.__reference__.open('w') as fp:
            jsonld_ = self.asjsonld()
            yaml.dump(jsonld_, fp, default_flow_style=False, Dumper=dumper)


s = attrs
ib = attrib
