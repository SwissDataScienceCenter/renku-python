# Copyright Swiss Data Science Center (SDSC)
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
"""Domain model for cloud storage."""
from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional


@dataclass
class CloudStorage:
    """A cloud storage definition.

    Cloud storages are defined on the storage service to easily reuse storage configurations (RClone) in projects.
    """

    name: str
    source_path: str
    target_path: str
    configuration: Dict[str, Any]
    storage_id: Optional[str] = None
    project_id: Optional[str] = None
    _storage_type: Optional[str] = None

    @property
    def storage_type(self) -> str:
        """The type of storage e.g. S3."""
        return self._storage_type or self.configuration["type"]

    @property
    def private(self) -> bool:
        """Whether the storage needs credentials or not."""
        return any(v == "<sensitive>" for _, v in self.configuration.items())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CloudStorage":
        """Instantiate from a dict."""
        return CloudStorage(
            storage_id=data["storage_id"],
            name=data["name"],
            source_path=data["source_path"],
            target_path=data["target_path"],
            configuration=data["configuration"],
            project_id=data.get("project_id"),
        )


CloudStorageWithSensitiveFields = NamedTuple(
    "CloudStorageWithSensitiveFields", [("storage", CloudStorage), ("private_fields", List[Dict[str, Any]])]
)
