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
"""Tests for storage service."""

from unittest.mock import MagicMock

import renku.infrastructure.storage.storage_service as storage_service
from renku.command.command_builder.command import inject, remove_injector
from renku.core.interface.git_api_provider import IGitAPIProvider


def test_storage_service_list(monkeypatch):
    """Test listing storage."""
    inject.configure(lambda binder: binder.bind(IGitAPIProvider, MagicMock()), bind_in_runtime=False)

    try:
        with monkeypatch.context() as monkey:

            def _send_request(*_, **__):
                return [
                    {
                        "storage": {
                            "storage_id": "ABCDEFG",
                            "name": "mystorage",
                            "source_path": "source/path",
                            "target_path": "target/path",
                            "private": True,
                            "configuration": {"type": "s3", "endpoint": "example.com"},
                        },
                        "sensitive_fields": {},
                    }
                ]

            monkey.setattr(storage_service.StorageService, "_send_request", _send_request)
            monkey.setattr(storage_service, "get_renku_url", lambda: "http://example.com")
            svc = storage_service.StorageService()
            storages = svc.list("123456")
            assert len(storages) == 1
            assert storages[0].storage.name == "mystorage"
            assert storages[0].storage.storage_type == "s3"

    finally:
        remove_injector()
