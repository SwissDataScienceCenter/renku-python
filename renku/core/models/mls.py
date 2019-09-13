import configparser
import datetime
import re
import uuid
from functools import partial
from pathlib import Path

import attr
from attr.validators import instance_of
from dateutil.parser import parse as parse_date

from renku.core import errors
from renku.core.models.entities import Entity
from renku.core.utils.doi import is_doi

from renku.core.models import jsonld

NoneType = type(None)

_path_attr = partial(
    jsonld.ib,
    converter=Path,
)

def _convert_implementation(obj):
    """Convert implementation object."""
    if isinstance(obj, dict):
        return Implementation.from_jsonld(obj)


def _convert_algorithm(obj):
    """Convert implementation object."""
    if isinstance(obj, dict):
        return Algorithm.from_jsonld(obj)


def _convert_input(value):
    """Convert input object."""
    input_values = []
    for v in value:
        if 'value' in v:
            input_values.append(HyperParameterSetting.from_jsonld(v))
    return input_values


def _convert_parameter_value(obj):
    """Convert parameter value."""
    if isinstance(obj, dict):
        # TODO: should be interpreted type value
        return obj["@value"]


def _convert_parameters(value):
    """Convert hyperparameter object."""
    return [HyperParameter.from_jsonld(v) for v in value]


@jsonld.s(
    type='mls:HyperParameter',
    slots=True,
    context={'mls': 'http://www.w3.org/ns/mls#'}
)
class HyperParameter:
    _id = jsonld.ib(default=None, context='@id', kw_only=True)


@jsonld.s(
    type='mls:Algorithm',
    slots=True,
    context={'mls': 'http://www.w3.org/ns/mls#'}
)
class Algorithm:
    _id = jsonld.ib(default=None, context='@id', kw_only=True)


@jsonld.s(
    type='mls:HyperParameterSetting',
    slots=True,
    context={
        'mls': 'http://www.w3.org/ns/mls#',
        'specified_by': 'mls:specifiedBy'
    }
)
class HyperParameterSetting:
    value = jsonld.ib(
        default=None,
        converter=_convert_parameter_value,
        context='mls:hasValue',
        kw_only=True
    )
    specified_by = jsonld.ib(default=None, context='mls:specifiedBy', kw_only=True)


@jsonld.s(
    type='mls:Implementation',
    context={
        'mls': 'http://www.w3.org/ns/mls#',
        'dcterms': 'http://purl.org/dc/terms/'
    }
)
class Implementation:
    """Repesent an ML Schema defined Model."""

    EDITABLE_FIELDS = []

    _id = jsonld.ib(default=None, context='@id', kw_only=True)

    identifier = jsonld.ib(
        default=attr.Factory(uuid.uuid4),
        context='schema:identifier',
        kw_only=True,
        converter=str
    )

    name = jsonld.ib(
        default=None, type=str, context='dcterms:title', kw_only=True
    )

    parameters = jsonld.container.list(
        HyperParameter,
        default=None,
        converter=_convert_parameters,
        context='mls:hasHyperParameter',
        kw_only=True
    )

    implements = jsonld.ib(
        default=None,
        context='mls:implements',
        type=Algorithm,
        converter=_convert_algorithm,
        kw_only=True)

    version = jsonld.ib(default=None, context='dcterms:hasVersion', kw_only=True)

    @property
    def display_name(self):
        """Get dataset display name."""
        name = re.sub(' +', ' ', self.name.lower()[:24])

        def to_unix(el):
            """Parse string to unix friendly name."""
            parsed_ = re.sub('[^a-zA-Z0-9]', '', re.sub(' +', ' ', el))
            parsed_ = re.sub(' .+', '.', parsed_.lower())
            return parsed_

        short_name = [to_unix(el) for el in name.split()]

        if self.version:
            version = to_unix(self.version)
            name = '{0}_{1}'.format('_'.join(short_name), version)
            return name

        return '.'.join(short_name)

    @property
    def uid(self):
        """UUID part of identifier."""
        return self.identifier.split('/')[-1]

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self._id):
            return self._id
        return str(self.uid)[:8]

    @property
    def editable(self):
        """Subset of attributes which user can edit."""
        obj = self.asjsonld()
        data = {field_: obj.pop(field_) for field_ in self.EDITABLE_FIELDS}
        return data


@jsonld.s(
    type='mls:Run',
    context={
        'mls': 'http://www.w3.org/ns/mls#',
        'dcterms': 'http://purl.org/dc/terms/'
    },
)
class Run(Entity):
    identifier = jsonld.ib(
        default=attr.Factory(uuid.uuid4),
        context='schema:identifier',
        kw_only=True,
        converter=str
    )

    executes = jsonld.ib(
        default=None,
        type=Implementation,
        converter=_convert_implementation,
        context='mls:executes',
        kw_only=True
    )

    input_values = jsonld.container.list(
        HyperParameterSetting,
        converter=_convert_input,
        context='mls:hasInput',
        kw_only=True
    )

    version = jsonld.ib(default=None, context='dcterms:hasVersion', kw_only=True)

    name = jsonld.ib(
        default=None, type=str, context='dcterms:title', kw_only=True
    )

