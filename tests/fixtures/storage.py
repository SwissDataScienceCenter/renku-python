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
"""Storage fixtures."""
from renku.core.interface.storage_service_gateway import IStorageService
from renku.domain_model.cloud_storage import CloudStorage, CloudStorageWithSensitiveFields


class DummyStorageService(IStorageService):
    """Dummy storage service."""

    @property
    def project_id(self):
        """Get a dummy project id."""
        return "123456"

    def list(self, project_id):
        """List dummy storage definition."""
        return [
            CloudStorageWithSensitiveFields(
                CloudStorage(
                    name="mystorage",
                    source_path="source",
                    target_path="target/path",
                    configuration={"type": "s3", "endpoint": "example.com"},
                    storage_id="ABCDEFG",
                    project_id="123456",
                ),
                [],
            )
        ]

    def create(self, storage):
        """Create storage."""
        raise NotImplementedError()

    def edit(self, storage_id, storage):
        """Edit storage."""
        raise NotImplementedError()

    def delete(self, storage_id):
        """Delete storage."""
        raise NotImplementedError()

    def validate(self, storage):
        """Validate storage."""
        raise NotImplementedError()
