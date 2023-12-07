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
"""Functionality for interacting with cloud storage."""

from pydantic import ConfigDict, validate_call

from renku.command.command_builder import inject
from renku.core.interface.storage_service_gateway import IStorageService


@inject.autoparams()
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def list_storage(storage_service: IStorageService):
    """List configured cloud storage for project."""
    project_id = storage_service.project_id
    if project_id is None:
        return []
    storages = storage_service.list(project_id)
    return storages
