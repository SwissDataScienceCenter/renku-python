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


def migrate_types(data):
    """Fix data types."""
    type_mapping = {
        'dcterms:creator': ['prov:Person', 'schema:Person'],
        'schema:Person': ['prov:Person', 'schema:Person'],
        str(sorted(['foaf:Project', 'prov:Location'])): [
            'prov:Location', 'schema:Project'
        ],
        'schema:DigitalDocument': [
            'prov:Entity', 'schema:DigitalDocument', 'wfprov:Artifact'
        ]
    }

    def replace_types(data):
        for key, value in data.items():
            if key == '@type':
                if not isinstance(value, str):
                    value = str(sorted(value))
                new_type = type_mapping.get(value)
                if new_type:
                    data[key] = new_type
            elif isinstance(value, dict):
                replace_types(value)
            elif isinstance(value, (list, tuple, set)):
                for v in value:
                    if isinstance(v, dict):
                        replace_types(v)

    replace_types(data)

    return data
