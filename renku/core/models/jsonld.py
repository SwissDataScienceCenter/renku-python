# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
from functools import partial
from importlib import import_module
from pathlib import Path

import attr
import pyld
import yaml
from attr._compat import iteritems
from attr._funcs import has
from attr._make import Factory, fields

from renku.core.models.locals import ReferenceMixin, with_reference

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

try:
    from yaml import CDumper as Dumper
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import Dumper, SafeLoader


# Shamelessly copy/pasting from SO:
# https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
# This is needed to allow us to load from yaml and use json down the line.
class NoDatesSafeLoader(SafeLoader):
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

    if '@version' not in context:
        context['@version'] = 1.1

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

        property_context, scoped_properties = _add_class_property_contexts(
            jsonld_cls, context, cls, types
        )

        context.update(property_context)

        jsonld_cls.__module__ = cls.__module__
        jsonld_cls._jsonld_type = types[0] if len(types) == 1 else list(
            sorted(set(types))
        )
        jsonld_cls._scoped_properties = scoped_properties
        jsonld_cls._renku_type = fullname(cls)

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
            type_ = pyld.jsonld.expand({
                '@type': jsonld_cls._jsonld_type,
                '@context': context
            })[0]['@type']
            if isinstance(type_, list):
                type_ = tuple(sorted(type_))
        except Exception:
            # FIXME make sure all classes have @id defined
            return jsonld_cls

        if (
            type_ in jsonld_cls.__type_registry__ and
            str(jsonld_cls.__type_registry__[type_]) != str(jsonld_cls)
        ):
            raise TypeError(
                'Type {0!r} in {1!r} is already registered for {2!r}.'.format(
                    jsonld_cls._jsonld_type,
                    jsonld_cls,
                    jsonld_cls.__type_registry__[type_],
                )
            )

        jsonld_cls.__type_registry__[type_] = jsonld_cls
        return jsonld_cls

    if maybe_cls is None:
        return wrap
    return wrap(maybe_cls)


def _add_class_property_contexts(
    jsonld_cls, context, original_cls, jsonld_ype
):
    """Adds ``@context`` of a class' properties to the class' ``@context``."""
    scoped_properties = []

    property_context = {}

    for a in attr.fields(jsonld_cls):
        key = a.name
        ctx = a.metadata.get(KEY)

        if ctx is None:
            continue

        current_context = None
        if isinstance(ctx, str) and ':' in ctx:
            prefix, _ = ctx.split(':', 1)
            if prefix in context:
                current_context = ctx

        elif isinstance(ctx, dict) or ctx not in context:
            current_context = ctx

        if KEY_CLS in a.metadata:
            t = a.metadata[KEY_CLS]
            current_context, is_scoped = _propagate_reference_contexts(
                t, current_context, ctx, original_cls, jsonld_ype
            )

            if is_scoped:
                scoped_properties.append(key)

        if current_context:
            property_context[key] = current_context

    return property_context, scoped_properties


def _propagate_reference_contexts(
    type_references, current_context, parent_context, parent_cls, parent_types
):
    """Get JSON-LD contexts for all types of a reference and propagate them."""
    if not isinstance(type_references, (list, set, tuple)):
        type_references = [type_references]
    classes = [import_class_from_string(c) for c in type_references]
    classes = [(c._jsonld_context, c._jsonld_type, c, True)
               for c in classes if hasattr(c, '_jsonld_context')]
    scoped_properties = False

    parent_cls_string = '{}.{}'.format(
        parent_cls.__module__, parent_cls.__name__
    )

    if parent_cls in type_references or parent_cls_string in type_references:
        # handle cases where a class can be a parent/child of itself
        classes.append((parent_context, parent_types, parent_cls, False))

    if len(classes) == 1:
        merge_ctx = classes[0][0]

        if not current_context:
            current_context = {'@id': parent_context}
        elif not isinstance(current_context, dict):
            current_context = {'@id': current_context}
        if classes[0][3]:
            current_context['@context'] = merge_ctx
    else:
        scoped_properties = True

        for merge_ctx, subtypes, cls, subcontext in classes:

            if not current_context:
                current_context = {'@id': parent_context}
            elif not isinstance(current_context, dict):
                current_context = {'@id': current_context}

            if '@context' not in current_context:
                current_context['@context'] = []

            if not isinstance(subtypes, (tuple, list)):
                subtypes = [subtypes]

            for subtype in subtypes:
                # Use nested, type scoped contexts for each semantic type
                # of a reference, to uniquely bind a context to a type.
                # We need to expand the subtype, as type scoped contexts
                # behave weirdly

                expanded_subtype = subtype

                prefix, suffix = subtype.split(':', 1)

                if prefix in merge_ctx:
                    expanded_subtype = '{}{}'.format(merge_ctx[prefix], suffix)

                subtype = subtype.replace(':', '_')
                if subcontext:
                    current_context['@context'].append({
                        fullname(cls) + '_' + subtype: {
                            '@id': expanded_subtype,
                            '@context': merge_ctx
                        },
                        '@version': 1.1
                    })
                else:
                    current_context['@context'].append({
                        fullname(cls) + '_' + subtype: {
                            '@id': expanded_subtype
                        },
                        '@version': 1.1
                    })

    return current_context, scoped_properties


def _default_converter(cls, value):
    """A default converter method that tries to deserialize objects."""
    cls = import_class_from_string(cls)
    if isinstance(value, dict):
        return cls.from_jsonld(value)

    return value


def _default_multi_converter(types, value):
    """A default converter for properties that can have several types."""
    if isinstance(value, dict) and '@type' in value:
        expanded = pyld.jsonld.expand(value)[0]
        type_ = expanded['@type']
        if not isinstance(type_, list):
            type_ = [type_]
        # If a json-ld class has multiple types, they are in a
        # sorted tuple. This is used as the key for the class
        # registry, so we have to match it here.
        type_ = tuple(sorted(type_))
        if type_ in JSONLDMixin.__type_registry__:
            new_cls = JSONLDMixin.__type_registry__[type_]
            if all(str(t) not in str(new_cls) for t in types):
                raise NotImplementedError(
                    'Unexpected property type {},  expected one of {}'.format(
                        new_cls, ','.join(types)
                    )
                )
            # expand and compact with relevant context
            value = pyld.jsonld.compact(value, new_cls._jsonld_context)
            return new_cls.from_jsonld(value)
    return value


def attrib(context=None, type=None, **kwargs):
    """Create a new attribute with context."""
    kwargs.setdefault('metadata', {})
    kwargs['metadata'][KEY] = context
    if type:
        kwargs['metadata'][KEY_CLS] = type

        if 'converter' not in kwargs:
            if hasattr(type, 'from_jsonld') or isinstance(type, str):
                kwargs['converter'] = partial(_default_converter, type)
            elif isinstance(type, list):
                kwargs['converter'] = partial(_default_multi_converter, type)

    return attr.ib(**kwargs)


_container_types = (
    (
        'list', list,
        lambda type, value: [_default_converter(type, v) for v in value]
    ),
    (
        'set', set,
        lambda type, value: {_default_converter(type, v)
                             for v in value}
    ),
    (
        'index', dict, lambda type, value:
        {k: _default_converter(type, v)
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
            elif isinstance(value, dict) and '@type' in value:
                # Collection might have been compacted away, wrap value
                return mapper(type, container([value]))
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
    add_context=True,
    use_scoped_type_form=False,
    basedir=None,
):
    """Dump a JSON-LD class to the JSON with generated ``@context`` field."""
    as_jsonld = getattr(inst, 'as_jsonld', None)
    if as_jsonld is not None and type(as_jsonld.__self__) in type(inst).mro():
        return inst.as_jsonld()

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

    inst_cls = type(inst)

    for a in attrs:
        v = getattr(inst, a.name)
        scoped = a.name in inst_cls._scoped_properties

        # skip proxies
        if isinstance(v, weakref.ReferenceType):
            continue

        if filter is not None and not filter(a, v):
            continue
        if recurse is True:
            if has(v.__class__):
                rv[a.name] = asjsonld(
                    v,
                    recurse=True,
                    filter=filter,
                    dict_factory=dict_factory,
                    add_context=False,
                    use_scoped_type_form=scoped,
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
                        add_context=False,
                        use_scoped_type_form=scoped,
                        basedir=basedir,
                    ) if has(i.__class__) else i for i in v
                ])
            elif isinstance(v, dict):
                df = dict_factory
                rv[a.name] = df((
                    asjsonld(
                        kk,
                        dict_factory=df,
                        add_context=False,
                        basedir=basedir,
                    ) if has(kk.__class__) else convert_value(kk),
                    asjsonld(
                        vv,
                        dict_factory=df,
                        add_context=False,
                        basedir=basedir,
                    ) if has(vv.__class__) else vv
                ) for kk, vv in iteritems(v))
            else:
                rv[a.name] = convert_value(v)
        else:
            rv[a.name] = convert_value(v)

    if add_context:
        rv['@context'] = deepcopy(inst_cls._jsonld_context)

    rv_type = []
    if inst_cls._jsonld_type:
        if isinstance(inst_cls._jsonld_type, (list, tuple, set)):
            rv_type.extend(inst_cls._jsonld_type)
        else:
            rv_type.append(inst_cls._jsonld_type)

        if use_scoped_type_form:
            rv_type = [
                '{}_{}'.format(inst_cls._renku_type, t.replace(':', '_'))
                for t in rv_type
            ]

        rv['@type'] = rv_type[0] if len(rv_type) == 1 else rv_type

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
            expanded = pyld.jsonld.expand(data)[0]
            type_ = expanded['@type']
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
            data = pyld.jsonld.compact(data, cls._jsonld_translate)
            # compact using the class json-ld context
            data.pop('@context', None)
            data = pyld.jsonld.compact(data, cls._jsonld_context)

        data.setdefault('@context', cls._jsonld_context)

        if data['@context'] != cls._jsonld_context:
            # merge new context into old context to prevent properties
            # getting lost in jsonld expansion
            if isinstance(data['@context'], str):
                data['@context'] = {'@base': data['@context']}
            data['@context'].update(cls._jsonld_context)
            try:
                compacted = pyld.jsonld.compact(data, cls._jsonld_context)
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
                no_value_context = isinstance(v, dict) and '@context' not in v
                has_nested_context = (
                    k in compacted['@context'] and
                    '@context' in compacted['@context'][k]
                )
                if no_value_context and has_nested_context:
                    # Propagate down context
                    v['@context'] = compacted['@context'][k]['@context']

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
        source = read_yaml(path)

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
        jsonld_ = self.asjsonld()
        write_yaml(path=self.__reference__, data=jsonld_)


def read_yaml(path):
    """Load YAML file and return its content as a dict."""
    with Path(path).open(mode='r') as fp:
        return yaml.load(fp, Loader=NoDatesSafeLoader) or {}


def write_yaml(path, data):
    """Store data to a YAML file."""

    Dumper.ignore_aliases = lambda _, data: True

    with Path(path).open('w') as fp:
        yaml.dump(data, fp, default_flow_style=False, Dumper=Dumper)


def fullname(cls):
    """Gets the fully qualified type name of this class."""
    if type(cls) != type:
        cls = type(cls)
    module = cls.__module__
    if module is None or module == str.__class__.__module__:
        return cls.__name__  # Avoid reporting __builtin__
    else:
        return '.'.join([module, cls.__name__])


def import_class_from_string(dotted_path):
    """Imports a fully qualified class string."""
    if not isinstance(dotted_path, str):
        return dotted_path
    module_path, class_name = dotted_path.rsplit('.', 1)
    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError:
        return None


s = attrs
ib = attrib
