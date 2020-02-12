# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Convert models to Common Workflow Language."""

import os
from pathlib import Path

import attr
from attr._compat import iteritems
from attr._funcs import has
from attr._make import fields
from pyld import jsonld

from renku.core.models.jsonld import asjsonld
from renku.core.models.locals import ReferenceMixin, with_reference


class CWLType(type):
    """Register CWL types."""

    def __init__(cls, name, bases, namespace):
        """Register CWL types."""
        super(CWLType, cls).__init__(name, bases, namespace)
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        if name in cls.registry:
            raise ValueError('Duplicate CWL class {0} definition'.format(name))
        cls.registry[name] = cls


def type_from_metadata(metadata_type, value):
    """Instantiates a metadata class from cwl metadata."""
    context = None

    if '@context' in value:
        context = {'@context': value['@context']}
    elif hasattr(metadata_type, '_jsonld_context'):
        context = {'@context': metadata_type._jsonld_context}

    if context:
        value = jsonld.compact(value, context)
    return metadata_type.from_jsonld(value)


class CWLClass(ReferenceMixin, metaclass=CWLType):
    """Include ``class`` field in serialized object."""

    @classmethod
    def from_cwl(cls, data, __reference__=None):
        """Return an instance from CWL data."""
        exclude_properties = ['class', '$namespaces', '@reverse']
        class_name = data.get('class', None)
        cls = cls.registry.get(class_name, cls)

        if '$namespaces' in data:
            # handle custom metadata
            keys = data.keys()

            metadata_keys = [(k, False) for k in keys if ':' in k]

            if '@reverse' in keys:
                metadata_keys.extend((k, True) for k in data['@reverse'].keys()
                                     if ':' in k)

            attrs = fields(cls)

            for a in attrs:
                # map custom metadata
                if 'cwl_metadata' not in a.metadata:
                    continue

                metadata = a.metadata['cwl_metadata']

                k = (metadata.get('property'), metadata.get('reverse', False))

                if k not in metadata_keys:
                    continue

                metadata_type = metadata.get('type')

                if not metadata_type:
                    raise ValueError('CWL metadata type not specified')

                if metadata.get('reverse', False):
                    metadata_value = data['@reverse'][metadata['property']]
                else:
                    metadata_value = data[metadata['property']]
                    exclude_properties.append(metadata['property'])

                if isinstance(metadata_value, list):
                    data[a.name] = [
                        type_from_metadata(metadata_type, v)
                        for v in metadata_value
                    ]
                else:
                    data[a.name
                         ] = type_from_metadata(metadata_type, metadata_value)

        if __reference__:
            with with_reference(__reference__):
                self = cls(
                    **{
                        k: v
                        for k, v in iteritems(data)
                        if k not in exclude_properties
                    }
                )
        else:
            self = cls(
                **{
                    k: v
                    for k, v in iteritems(data) if k not in exclude_properties
                }
            )
        return self

    @classmethod
    def from_yaml(cls, path):
        """Return an instance from a YAML file."""
        import yaml

        with path.open(mode='r') as fp:
            self = cls.from_cwl(yaml.safe_load(fp), __reference__=path)

        return self


def mapped(cls, key='id', **kwargs):
    """Create list of instances from a mapping."""
    kwargs.setdefault('metadata', {})
    kwargs['metadata']['jsonldPredicate'] = {'mapSubject': key}
    kwargs.setdefault('default', attr.Factory(list))

    def converter(value):
        """Convert mapping to a list of instances."""
        if isinstance(value, dict):
            result = []
            for k, v in iteritems(value):
                if not hasattr(cls, 'from_cwl'):
                    vv = dict(v)
                    vv[key] = k
                else:
                    vv = attr.evolve(cls.from_cwl(v), **{key: k})
                result.append(vv)
        else:
            result = value

        def fix_keys(data):
            """Fix names of keys."""
            for a in fields(cls):
                a_name = a.name.rstrip('_')
                if a_name in data:
                    yield a.name, data[a_name]

        return [
            cls(**{kk: vv
                   for kk, vv in fix_keys(v)}) if not isinstance(v, cls) else v
            for v in result
        ]

    kwargs['converter'] = converter
    return attr.ib(**kwargs)


def ascwl(
    inst,
    recurse=True,
    filter=None,
    dict_factory=dict,
    retain_collection_types=False,
    basedir=None,
):
    """Return the ``attrs`` attribute values of *inst* as a dict.

    Support ``jsonldPredicate`` in a field metadata for generating
    mappings from lists.

    Adapted from ``attr._funcs``.
    """
    attrs = fields(inst.__class__)
    rv = dict_factory()

    def convert_value(v):
        """Convert special types."""
        if isinstance(v, Path):
            v = str(v)
            return os.path.relpath(v, str(basedir)) if basedir else v
        return v

    def convert_cwl_metadata(rv, v, meta):
        """Convert custom metadata to cwl."""
        if not v:
            return

        if isinstance(v, (list, tuple)):
            result = [asjsonld(vv) for vv in v]
        else:
            result = asjsonld(v)

        if 'reverse' in meta and meta['reverse']:
            if '@reverse' not in rv:
                rv['@reverse'] = {}
            rv['@reverse'][meta['property']] = result
        else:
            rv[meta['property']] = result

        if '$namespaces' not in rv:
            rv['$namespaces'] = {}

        rv['$namespaces'][meta['prefix']] = meta['namespace']

    for a in attrs:
        if a.name.startswith('__'):
            continue

        a_name = a.name.rstrip('_')
        v = getattr(inst, a.name)
        if filter is not None and not filter(a, v):
            continue
        if 'cwl_metadata' in a.metadata:
            convert_cwl_metadata(rv, v, a.metadata['cwl_metadata'])
            continue
        if recurse is True:
            if has(v.__class__):
                rv[a_name] = ascwl(
                    v,
                    recurse=True,
                    filter=filter,
                    dict_factory=dict_factory,
                    basedir=basedir,
                )

            elif isinstance(v, (tuple, list, set)):
                cf = v.__class__ if retain_collection_types is True else list
                rv[a_name] = cf([
                    ascwl(
                        i,
                        recurse=True,
                        filter=filter,
                        dict_factory=dict_factory,
                        basedir=basedir,
                    ) if has(i.__class__) else i for i in v
                ])

                if 'jsonldPredicate' in a.metadata:
                    k = a.metadata['jsonldPredicate'].get('mapSubject')
                    if k:
                        vv = dict_factory()
                        for i in rv[a_name]:
                            kk = i.pop(k)
                            vv[kk] = i
                        rv[a_name] = vv

            elif isinstance(v, dict):
                df = dict_factory
                rv[a_name] = df((
                    ascwl(
                        kk,
                        dict_factory=df,
                        basedir=basedir,
                    ) if has(kk.__class__) else convert_value(kk),
                    ascwl(
                        vv,
                        dict_factory=df,
                        basedir=basedir,
                    ) if has(vv.__class__) else vv
                ) for kk, vv in iteritems(v))
            else:
                rv[a_name] = convert_value(v)
        else:
            rv[a_name] = convert_value(v)

    if isinstance(inst, CWLClass):
        rv['class'] = inst.__class__.__name__

    return rv
