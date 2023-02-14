# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Migration models V7."""

import os

from marshmallow import EXCLUDE

from renku.command.schema.calamus import fields, prov, renku, schema
from renku.core.migration.utils import OLD_METADATA_PATH, get_datasets_path
from renku.core.util import yaml

from .v3 import Base, DatasetFileSchemaV3, DatasetSchemaV3, UrlSchemaV3


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, commit=None):
        """Read content from YAML file."""
        data = yaml.read_yaml(path)
        self = DatasetSchemaV7(commit=commit).load(data)
        self._metadata_path = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        data = DatasetSchemaV7().dump(self)
        path = path or self._metadata_path or os.path.join(self.path, OLD_METADATA_PATH)
        yaml.write_yaml(path=path, data=data)


class DatasetFileSchemaV7(DatasetFileSchemaV3):
    """DatasetFile schema."""

    based_on = fields.Nested(schema.isBasedOn, "DatasetFileSchemaV7", load_default=None)
    source = fields.String(renku.source, load_default=None)


class DatasetSchemaV7(DatasetSchemaV3):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    derived_from = fields.Nested(prov.wasDerivedFrom, UrlSchemaV3, load_default=None)
    files = fields.Nested(schema.hasPart, DatasetFileSchemaV7, many=True)


def get_project_datasets():
    """Return Dataset migration models for a project."""
    paths = get_datasets_path().rglob(OLD_METADATA_PATH)
    return [Dataset.from_yaml(path=path) for path in paths]
