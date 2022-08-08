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
"""Storage factory implementation."""

from typing import TYPE_CHECKING

from renku.core import errors
from renku.core.interface.storage import IStorage, IStorageFactory
from renku.core.util.urls import get_scheme

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials


class StorageFactory(IStorageFactory):
    """Return an external storage."""

    @staticmethod
    def get_storage(provider: "ProviderApi", credentials: "ProviderCredentials") -> "IStorage":
        """Return a storage that handles provider."""
        from .s3 import S3Storage

        storage_handlers = {"s3": S3Storage}

        scheme = get_scheme(uri=provider.uri).lower()

        if scheme not in storage_handlers:
            raise errors.StorageProviderNotFound(provider.uri)

        return storage_handlers[scheme](provider=provider, credentials=credentials)
