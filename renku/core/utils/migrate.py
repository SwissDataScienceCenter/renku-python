# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Helper utils for migrations."""

import pyld

from renku.core.models.jsonld import read_yaml


def migrate_types(data):
    """Fix data types."""
    type_mapping = {
        "dcterms:creator": ["prov:Person", "schema:Person"],
        "schema:Person": ["prov:Person", "schema:Person"],
        str(sorted(["foaf:Project", "prov:Location"])): ["prov:Location", "schema:Project"],
        "schema:DigitalDocument": ["prov:Entity", "schema:DigitalDocument", "wfprov:Artifact"],
    }

    def remove_type(data_):
        data_.pop("@type", None)
        for key, value in data_.items():
            if isinstance(value, dict):
                remove_type(value)
            elif isinstance(value, (list, tuple, set)):
                for v in value:
                    if isinstance(v, dict):
                        remove_type(v)

    def replace_types(data_):
        for key, value in data_.items():
            if key == "@context" and isinstance(value, dict):
                remove_type(value)

            if key == "@type":
                if not isinstance(value, str):
                    value = str(sorted(value))
                new_type = type_mapping.get(value)
                if new_type:
                    data_[key] = new_type
            elif isinstance(value, dict):
                replace_types(value)
            elif isinstance(value, (list, tuple, set)):
                for v in value:
                    if isinstance(v, dict):
                        replace_types(v)

    replace_types(data)

    return data


def get_pre_0_3_4_datasets_metadata(client):
    """Return paths of dataset metadata for pre 0.3.4."""
    from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR

    project_is_pre_0_3 = int(read_project_version(client)) < 2
    if project_is_pre_0_3:
        return (client.path / DATA_DIR).glob(f"*/{client.METADATA}")
    return []


def read_project_version(client):
    """Read project version from metadata file."""
    yaml_data = read_yaml(client.renku_metadata_path)
    return read_project_version_from_yaml(yaml_data)


def read_project_version_from_yaml(yaml_data):
    """Read project version from YAML data."""
    jsonld = pyld.jsonld.expand(yaml_data)[0]
    jsonld = normalize(jsonld)
    return _get_jsonld_property(jsonld, "http://schema.org/schemaVersion", "1")


def _get_jsonld_property(jsonld, property, default=None):
    """Return property value from expanded JSON-LD data."""
    value = jsonld.get(property)
    if not value:
        return default
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict) and "@value" in value[0]:
        value = value[0]["@value"]
    return value


def normalize(value):
    """Normalize an expanded JSON-LD."""
    if isinstance(value, list):
        if len(value) == 1 and isinstance(value[0], dict) and "@value" in value[0]:
            return value[0]["@value"]
        return [normalize(v) for v in value]

    if isinstance(value, dict):
        if "@value" in value:
            return value["@value"]
        else:
            return {k: normalize(v) for k, v in value.items()}

    return value
