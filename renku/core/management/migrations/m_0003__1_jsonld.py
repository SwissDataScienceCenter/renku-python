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
"""JSON-LD dataset migrations."""
import itertools
import json
import os
import uuid
from pathlib import Path

import pyld

from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.jsonld import read_yaml, write_yaml


def migrate(client):
    """Migration function."""
    _migrate_project_metadata(client)
    _migrate_datasets_metadata(client)


def _migrate_project_metadata(client):
    """Apply all initial JSON-LD migrations to project."""
    jsonld_translate = {
        "http://schema.org/name": "http://xmlns.com/foaf/0.1/name",
        "http://schema.org/Project": "http://xmlns.com/foaf/0.1/Project",
    }

    _apply_on_the_fly_jsonld_migrations(
        path=client.renku_metadata_path,
        jsonld_context=_INITIAL_JSONLD_PROJECT_CONTEXT,
        fields=_PROJECT_FIELDS,
        jsonld_translate=jsonld_translate,
    )


def _migrate_datasets_metadata(client):
    """Apply all initial JSON-LD migrations to datasets."""
    jsonld_migrations = {
        "dctypes:Dataset": [_migrate_dataset_schema, _migrate_absolute_paths],
        "schema:Dataset": [
            _migrate_absolute_paths,
            _migrate_doi_identifier,
            _migrate_same_as_structure,
            _migrate_dataset_file_id,
        ],
    }

    old_metadata_paths = _dataset_pre_0_3(client)
    new_metadata_paths = client.renku_datasets_path.rglob(client.METADATA)

    for path in itertools.chain(old_metadata_paths, new_metadata_paths):
        _apply_on_the_fly_jsonld_migrations(
            path=path,
            jsonld_context=_INITIAL_JSONLD_DATASET_CONTEXT,
            fields=_DATASET_FIELDS,
            client=client,
            jsonld_migrations=jsonld_migrations,
        )


def _apply_on_the_fly_jsonld_migrations(
    path, jsonld_context, fields, client=None, jsonld_migrations=None, jsonld_translate=None
):
    data = read_yaml(path)

    if jsonld_translate:
        # perform the translation

        data = pyld.jsonld.expand(data)
        data_str = json.dumps(data)
        for k, v in jsonld_translate.items():
            data_str = data_str.replace(v, k)
        data = json.loads(data_str)
        data = pyld.jsonld.compact(data, jsonld_context)

    data.setdefault("@context", jsonld_context)

    _migrate_types(data)

    if jsonld_migrations:
        schema_type = data.get("@type")
        migrations = []

        if isinstance(schema_type, list):
            for schema in schema_type:
                migrations += jsonld_migrations.get(schema, [])
        elif isinstance(schema_type, str):
            migrations += jsonld_migrations.get(schema_type, [])

        for migration in set(migrations):
            data = migration(data, client)

    if data["@context"] != jsonld_context:
        # merge new context into old context to prevent properties
        # getting lost in jsonld expansion
        if isinstance(data["@context"], str):
            data["@context"] = {"@base": data["@context"]}
        data["@context"].update(jsonld_context)
        try:
            compacted = pyld.jsonld.compact(data, jsonld_context)
        except Exception:
            compacted = data
    else:
        compacted = data

    data = {}

    for k, v in compacted.items():
        if k in fields:
            no_value_context = isinstance(v, dict) and "@context" not in v
            has_nested_context = k in compacted["@context"] and "@context" in compacted["@context"][k]
            if no_value_context and has_nested_context:
                # Propagate down context
                v["@context"] = compacted["@context"][k]["@context"]

            data[k] = v

    data["@context"] = jsonld_context

    write_yaml(path, data)


def _dataset_pre_0_3(client):
    """Return paths of dataset metadata for pre 0.3.4."""
    project_is_pre_0_3 = int(client.project.version) < 2
    if project_is_pre_0_3:
        return (client.path / DATA_DIR).rglob(client.METADATA)
    return []


def _migrate_dataset_schema(data, client):
    """Migrate from old dataset formats."""
    if "authors" not in data:
        return

    data["@context"]["creator"] = data["@context"].pop("authors", {"@container": "list"})

    data["creator"] = data.pop("authors", {})

    files = data.get("files", [])

    if isinstance(files, dict):
        files = files.values()
    for file_ in files:
        file_["creator"] = file_.pop("authors", {})

    return data


def _migrate_absolute_paths(data, client):
    """Migrate dataset paths to use relative path."""
    raw_path = data.get("path", ".")
    path = Path(raw_path)

    if path.is_absolute():
        try:
            data["path"] = str(path.relative_to(os.getcwd()))
        except ValueError:
            elements = raw_path.split("/")
            index = elements.index(".renku")
            data["path"] = str(Path("/".join(elements[index:])))

    files = data.get("files", [])

    if isinstance(files, dict):
        files = list(files.values())

    for file_ in files:
        path = Path(file_.get("path"), ".")
        if path.is_absolute():
            file_["path"] = str(path.relative_to((os.getcwd())))
    data["files"] = files
    return data


def _migrate_doi_identifier(data, client):
    """If the dataset _id is doi, make it a UUID."""
    from renku.core.utils.doi import is_doi
    from renku.core.utils.uuid import is_uuid

    _id = data.get("_id", "")
    identifier = data.get("identifier", "")

    if not is_uuid(_id):
        if not is_uuid(identifier):
            data["identifier"] = str(uuid.uuid4())
        if is_doi(data.get("_id", "")):
            data["same_as"] = {"@type": ["schema:URL"], "url": data["_id"]}
            if data.get("@context"):
                data["@context"].setdefault(
                    "same_as",
                    {
                        "@id": "schema:sameAs",
                        "@type": "schema:URL",
                        "@context": {"@version": "1.1", "url": "schema:url", "schema": "http://schema.org/"},
                    },
                )
        data["_id"] = data["identifier"]
    return data


def _migrate_same_as_structure(data, client):
    """Changes sameAs string to schema:URL object."""
    same_as = data.get("same_as")

    if same_as and isinstance(same_as, str):
        data["same_as"] = {"@type": ["schema:URL"], "url": same_as}

        if data.get("@context"):
            data["@context"].setdefault(
                "same_as",
                {
                    "@id": "schema:sameAs",
                    "@type": "schema:URL",
                    "@context": {"@version": "1.1", "url": "schema:url", "schema": "http://schema.org/"},
                },
            )

    return data


def _migrate_dataset_file_id(data, client):
    """Ensure dataset files have a fully qualified url as id."""
    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    files = data.get("files", [])
    for file_ in files:
        if not file_["_id"].startswith("http"):
            file_["_id"] = "https://{host}/{id}".format(host=host, id=file_["_id"])

    return data


def _migrate_types(data):
    """Fix types."""
    from renku.core.utils.migrate import migrate_types

    migrate_types(data)


_PROJECT_FIELDS = {"_id", "created", "creator", "name", "updated", "version"}

_DATASET_FIELDS = {
    "_id",
    "_label",
    "_project",
    "based_on",
    "created",
    "creator",
    "date_published",
    "description",
    "files",
    "identifier",
    "in_language",
    "keywords",
    "license",
    "name",
    "path",
    "same_as",
    "short_name",
    "tags",
    "url",
    "version",
}

_INITIAL_JSONLD_PROJECT_CONTEXT = {
    "schema": "http://schema.org/",
    "prov": "http://www.w3.org/ns/prov#",
    "@version": 1.1,
    "name": "schema:name",
    "created": "schema:dateCreated",
    "updated": "schema:dateUpdated",
    "version": "schema:schemaVersion",
    "creator": {
        "@id": "schema:creator",
        "@context": {
            "schema": "http://schema.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "@version": 1.1,
            "name": "schema:name",
            "email": "schema:email",
            "label": "rdfs:label",
            "affiliation": "schema:affiliation",
            "alternate_name": "schema:alternateName",
            "_id": "@id",
        },
    },
    "_id": "@id",
}

_INITIAL_JSONLD_DATASET_CONTEXT = {
    "schema": "http://schema.org/",
    "@version": 1.1,
    "prov": "http://www.w3.org/ns/prov#",
    "wfprov": "http://purl.org/wf4ever/wfprov#",
    "path": "prov:atLocation",
    "_id": "@id",
    "_project": {
        "@id": "schema:isPartOf",
        "@context": {
            "schema": "http://schema.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "@version": 1.1,
            "name": "schema:name",
            "created": "schema:dateCreated",
            "updated": "schema:dateUpdated",
            "version": "schema:schemaVersion",
            "creator": {
                "@id": "schema:creator",
                "@context": {
                    "schema": "http://schema.org/",
                    "prov": "http://www.w3.org/ns/prov#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "@version": 1.1,
                    "name": "schema:name",
                    "email": "schema:email",
                    "label": "rdfs:label",
                    "affiliation": "schema:affiliation",
                    "alternate_name": "schema:alternateName",
                    "_id": "@id",
                },
            },
            "_id": "@id",
        },
    },
    "creator": {
        "@id": "schema:creator",
        "@context": {
            "schema": "http://schema.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "@version": 1.1,
            "name": "schema:name",
            "email": "schema:email",
            "label": "rdfs:label",
            "affiliation": "schema:affiliation",
            "alternate_name": "schema:alternateName",
            "_id": "@id",
        },
    },
    "date_published": "schema:datePublished",
    "description": "schema:description",
    "identifier": "schema:identifier",
    "in_language": {
        "@id": "schema:inLanguage",
        "@context": {
            "schema": "http://schema.org/",
            "@version": 1.1,
            "alternate_name": "schema:alternateName",
            "name": "schema:name",
        },
    },
    "keywords": "schema:keywords",
    "based_on": "schema:isBasedOn",
    "license": "schema:license",
    "name": "schema:name",
    "url": "schema:url",
    "version": "schema:version",
    "created": "schema:dateCreated",
    "files": {
        "@id": "schema:hasPart",
        "@context": {
            "schema": "http://schema.org/",
            "@version": 1.1,
            "prov": "http://www.w3.org/ns/prov#",
            "wfprov": "http://purl.org/wf4ever/wfprov#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "path": "prov:atLocation",
            "_id": "@id",
            "_label": "rdfs:label",
            "_project": {
                "@id": "schema:isPartOf",
                "@context": {
                    "schema": "http://schema.org/",
                    "prov": "http://www.w3.org/ns/prov#",
                    "@version": 1.1,
                    "name": "schema:name",
                    "created": "schema:dateCreated",
                    "updated": "schema:dateUpdated",
                    "version": "schema:schemaVersion",
                    "creator": {
                        "@id": "schema:creator",
                        "@context": {
                            "schema": "http://schema.org/",
                            "prov": "http://www.w3.org/ns/prov#",
                            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                            "@version": 1.1,
                            "name": "schema:name",
                            "email": "schema:email",
                            "label": "rdfs:label",
                            "affiliation": "schema:affiliation",
                            "alternate_name": "schema:alternateName",
                            "_id": "@id",
                        },
                    },
                    "_id": "@id",
                },
            },
            "creator": {
                "@id": "schema:creator",
                "@context": {
                    "schema": "http://schema.org/",
                    "prov": "http://www.w3.org/ns/prov#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "@version": 1.1,
                    "name": "schema:name",
                    "email": "schema:email",
                    "label": "rdfs:label",
                    "affiliation": "schema:affiliation",
                    "alternate_name": "schema:alternateName",
                    "_id": "@id",
                },
            },
            "added": "schema:dateCreated",
            "name": "schema:name",
            "url": "schema:url",
            "external": "renku:external",
            "based_on": "schema:isBasedOn",
            "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        },
    },
    "tags": {
        "@id": "schema:subjectOf",
        "@context": {
            "schema": "http://schema.org/",
            "@version": 1.1,
            "name": "schema:name",
            "description": "schema:description",
            "commit": "schema:location",
            "created": "schema:startDate",
            "dataset": "schema:about",
            "_id": "@id",
        },
    },
    "same_as": {
        "@id": "schema:sameAs",
        "@context": {"schema": "http://schema.org/", "@version": 1.1, "url": "schema:url", "_id": "@id"},
    },
    "short_name": "schema:alternateName",
}
