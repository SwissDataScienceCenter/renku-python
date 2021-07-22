# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
import pathlib
import uuid
from urllib.parse import ParseResult, quote, urljoin, urlparse


def generate_url_id(client, url_str, url_id):
    """Generate @id field for Url."""
    url = url_str or url_id
    if url:
        parsed_result = urlparse(url)
        id_ = ParseResult("", *parsed_result[1:]).geturl()
    else:
        id_ = str(uuid.uuid4())

    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/urls", quote(id_, safe="")))


def generate_dataset_tag_id(client, name, commit):
    """Generate @id field for DatasetTag."""
    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    name = "{0}@{1}".format(name, commit)

    return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/datasettags", quote(name, safe="")))


def generate_dataset_id(client, identifier):
    """Generate @id field."""
    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    # always set the id by the identifier
    return urljoin(f"https://{host}", pathlib.posixpath.join("/datasets", quote(identifier, safe="")))


def generate_dataset_file_url(client, filepath):
    """Generate url for DatasetFile."""
    if not client or not client.project:
        return

    project_id = urlparse(client.project._id)
    filepath = quote(filepath, safe="/")
    path = pathlib.posixpath.join(project_id.path, "files", "blob", filepath)
    project_id = project_id._replace(path=path)

    return project_id.geturl()
