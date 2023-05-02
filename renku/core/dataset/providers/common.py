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
"""Common functionality for data providers."""

import re
from pathlib import Path
from typing import List

from renku.core import errors
from renku.core.dataset.providers.api import StorageProviderInterface
from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
from renku.domain_model.dataset import RemoteEntity
from renku.domain_model.project_context import project_context


def get_metadata(
    provider: StorageProviderInterface, uri: str, destination: Path, dataset_add_action: DatasetAddAction
) -> List["DatasetAddMetadata"]:
    """Add files from a URI to a dataset."""
    if re.search(r"[*?]", uri):
        raise errors.ParameterError("Wildcards like '*' or '?' are not supported for cloud storage URIs.")

    storage = provider.get_storage()

    destination_path_in_repo = Path(destination).relative_to(project_context.repository.path)
    hashes = storage.get_hashes(uri=uri)
    if dataset_add_action == DatasetAddAction.NONE:
        dataset_add_action = DatasetAddAction.REMOTE_STORAGE
    return [
        DatasetAddMetadata(
            entity_path=destination_path_in_repo / hash.path,
            url=hash.uri,
            action=dataset_add_action,
            based_on=RemoteEntity(checksum=hash.hash if hash.hash else "", url=hash.uri, path=hash.path),
            source=Path(hash.uri),
            destination=destination / hash.path,
            provider=provider,
            size=hash.size,
        )
        for hash in hashes
    ]
