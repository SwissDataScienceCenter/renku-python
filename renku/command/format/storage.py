# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Serializers for storage."""

import json
from typing import List, Optional

from renku.command.format.tabulate import tabulate
from renku.domain_model.cloud_storage import CloudStorage


def tabular(cloud_storages: List[CloudStorage], *, columns: Optional[str] = None):
    """Format cloud_storages with a tabular output."""
    if not columns:
        columns = "id,name,private,type"
    return tabulate(collection=cloud_storages, columns=columns, columns_mapping=CLOUD_STORAGE_COLUMNS)


def log(cloud_storages: List[CloudStorage], *, columns: Optional[str] = None):
    """Format cloud_storages in a log like output."""
    from renku.ui.cli.utils.terminal import style_header, style_key

    output = []

    for cloud_storage in cloud_storages:
        output.append(style_header(f"CloudStorage {cloud_storage.name}"))
        output.append(style_key("Id: ") + cloud_storage.storage_id)  # type: ignore
        output.append(style_key("Source Path: ") + cloud_storage.source_path)
        output.append(style_key("Target path: ") + cloud_storage.target_path)
        output.append(style_key("Private: ") + ("Yes" if cloud_storage.private else "No"))
        output.append(style_key("Configuration: \n") + json.dumps(cloud_storage.configuration, indent=4))
        output.append("")
    return "\n".join(output)


CLOUD_STORAGE_FORMATS = {"tabular": tabular, "log": log}
"""Valid formatting options."""

CLOUD_STORAGE_COLUMNS = {
    "id": ("storage_id", "id"),
    "name": ("name", "name"),
    "source_path": ("source_path", "source path"),
    "target_path": ("target_path", "target path"),
    "private": ("private", "private"),
    "type": ("storage_type", "type"),
}
