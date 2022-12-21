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
"""Migration utility functions."""

import os
import posixpath
import threading
import uuid
from urllib.parse import ParseResult, quote, urljoin, urlparse

from renku.core.util.yaml import read_yaml
from renku.domain_model.project_context import project_context

OLD_METADATA_PATH = "metadata.yml"
OLD_DATASETS_PATH = "datasets"
OLD_WORKFLOW_PATH = "workflow"

thread_local_storage = threading.local()


def generate_url_id(url_str, url_id):
    """Generate @id field for Url."""
    url = url_str or url_id
    if url:
        parsed_result = urlparse(url)
        id_ = ParseResult("", *parsed_result[1:]).geturl()
    else:
        id_ = str(uuid.uuid4())

    host = "localhost"
    if project_context.has_context():
        host = project_context.remote.host or host
    host = os.environ.get("RENKU_DOMAIN") or host

    return urljoin("https://{host}".format(host=host), posixpath.join("/urls", quote(id_, safe="")))


def generate_dataset_tag_id(name, commit):
    """Generate @id field for DatasetTag."""
    host = "localhost"
    if project_context.has_context():
        host = project_context.remote.host or host
    host = os.environ.get("RENKU_DOMAIN") or host

    name = "{0}@{1}".format(name, commit)

    return urljoin("https://{host}".format(host=host), posixpath.join("/dataset-tags", quote(name, safe="")))


def generate_dataset_id(identifier):
    """Generate @id field."""
    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"
    if project_context.has_context():
        host = project_context.remote.host or host
    host = os.environ.get("RENKU_DOMAIN") or host

    # always set the id by the identifier
    return urljoin(f"https://{host}", posixpath.join("/datasets", quote(identifier, safe="")))


def generate_dataset_file_url(filepath):
    """Generate url for DatasetFile."""
    try:
        project = project_context.project
        if not project:
            return
    except ValueError:
        from renku.core.migration.models.v9 import Project

        metadata_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
        project = Project.from_yaml(metadata_path)

        project_id = urlparse(project._id)
    else:
        project_id = urlparse(project.id)

    filepath = quote(filepath, safe="/")
    path = posixpath.join(project_id.path, "files", "blob", filepath)
    project_id = project_id._replace(path=path)

    return project_id.geturl()


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


def get_pre_0_3_4_datasets_metadata():
    """Return paths of dataset metadata for pre 0.3.4."""
    from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR

    project_is_pre_0_3 = int(read_project_version()) < 2
    if project_is_pre_0_3:
        return (project_context.path / DATA_DIR).glob(f"*/{OLD_METADATA_PATH}")
    return []


def read_project_version() -> str:
    """Read project version from metadata file."""
    try:
        return project_context.project.version
    except (NotImplementedError, ValueError):
        metadata_path = project_context.metadata_path / OLD_METADATA_PATH
        if not os.path.exists(metadata_path):
            return "1"

        yaml_data = read_yaml(metadata_path)
        return read_project_version_from_yaml(yaml_data)


def read_latest_agent():
    """Read project version from metadata file."""
    import pyld

    try:
        return project_context.latest_agent
    except (NotImplementedError, ValueError):
        metadata_path = project_context.metadata_path / OLD_METADATA_PATH
        if not os.path.exists(metadata_path):
            raise

        yaml_data = read_yaml(metadata_path)
        jsonld = pyld.jsonld.expand(yaml_data)[0]
        jsonld = normalize(jsonld)
        return _get_jsonld_property(jsonld, "http://schema.org/agent", "pre-0.11.0")


def read_project_version_from_yaml(yaml_data):
    """Read project version from YAML data."""
    import pyld

    jsonld = pyld.jsonld.expand(yaml_data)[0]
    jsonld = normalize(jsonld)
    return _get_jsonld_property(jsonld, "http://schema.org/schemaVersion", "1")


def _get_jsonld_property(jsonld, property_name, default=None):
    """Return property value from expanded JSON-LD data."""
    value = jsonld.get(property_name)
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


def get_datasets_path():
    """Get the old datasets metadata path."""
    return getattr(thread_local_storage, "temporary_datasets_path", project_context.metadata_path / OLD_DATASETS_PATH)


def set_temporary_datasets_path(temporary_datasets_path):
    """Set a temporary datasets metadata path."""
    thread_local_storage.temporary_datasets_path = temporary_datasets_path


def is_using_temporary_datasets_path():
    """Check if temporary datasets path is set."""
    return bool(getattr(thread_local_storage, "temporary_datasets_path", None))


def unset_temporary_datasets_path():
    """Unset the current temporary datasets metadata path."""
    if getattr(thread_local_storage, "temporary_datasets_path", None):
        del thread_local_storage.temporary_datasets_path
