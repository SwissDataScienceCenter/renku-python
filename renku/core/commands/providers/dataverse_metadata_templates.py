# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Dataverse metadata templates."""
DATASET_METADATA_TEMPLATE = """
{
    "datasetVersion": {
        "metadataBlocks": {
            "citation": {
                "fields": [
                    {
                        "value": "${name}",
                        "typeClass": "primitive",
                        "multiple": false,
                        "typeName": "title"
                    },
                    {
                        "value": ${authors},
                        "typeClass": "compound",
                        "multiple": true,
                        "typeName": "author"
                    },
                    {
                        "value": ${contacts},
                        "typeClass": "compound",
                        "multiple": true,
                        "typeName": "datasetContact"
                    },
                    {
                        "value": [
                            {
                                "dsDescriptionValue": {
                                    "value": "${description}",
                                    "multiple": false,
                                    "typeClass": "primitive",
                                    "typeName": "dsDescriptionValue"
                                }
                            }
                        ],
                        "typeClass": "compound",
                        "multiple": true,
                        "typeName": "dsDescription"
                    },
                    {
                        "value": [],
                        "typeClass": "controlledVocabulary",
                        "multiple": true,
                        "typeName": "subject"
                    }
                ],
                "displayName": "Citation Metadata"
            }
        }
    }
}"""

AUTHOR_METADATA_TEMPLATE = """
{
    "authorName": {
        "value": "${name}",
        "typeClass": "primitive",
        "multiple": false,
        "typeName": "authorName"
    },
    "authorAffiliation": {
        "value": "${affiliation}",
        "typeClass": "primitive",
        "multiple": false,
        "typeName": "authorAffiliation"
    }
}
"""

CONTACT_METADATA_TEMPLATE = """
{
    "datasetContactEmail": {
        "typeClass": "primitive",
        "multiple": false,
        "typeName": "datasetContactEmail",
        "value": "${email}"
    },
    "datasetContactName": {
        "typeClass": "primitive",
        "multiple": false,
        "typeName": "datasetContactName",
        "value": "${name}"
    }
}
"""
