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
"""Helper utilities for various dispatchers."""

from typing import TYPE_CHECKING

from renku.command.command_builder.command import inject
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.interface.storage import IStorageFactory

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials
    from renku.core.interface.storage import IStorage
    from renku.core.management.client import LocalClient
    from renku.infrastructure.database import Database
    from renku.infrastructure.repository import Repository


def get_client() -> "LocalClient":
    """Return current client."""

    @inject.autoparams()
    def get_client_helper(client_dispatcher: IClientDispatcher):
        return client_dispatcher.current_client

    return get_client_helper()


def get_database() -> "Database":
    """Return current database."""

    @inject.autoparams()
    def get_database_helper(database_dispatcher: IDatabaseDispatcher):
        return database_dispatcher.current_database

    return get_database_helper()


def get_repository() -> "Repository":
    """Return current project's repository."""
    return get_client().repository


def get_storage(provider: "ProviderApi", credentials: "ProviderCredentials") -> "IStorage":
    """Return a storage provider for the given URI."""

    @inject.autoparams()
    def get_storage_helper(storage_factory: IStorageFactory):
        return storage_factory.get_storage(provider=provider, credentials=credentials)

    return get_storage_helper()
