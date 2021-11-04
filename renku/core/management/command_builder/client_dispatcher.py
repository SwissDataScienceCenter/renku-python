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
"""Renku client dispatcher."""
from pathlib import Path
from typing import Optional, Union

from renku.core import errors
from renku.core.management import RENKU_HOME
from renku.core.management.client import LocalClient
from renku.core.management.interface.client_dispatcher import IClientDispatcher


class ClientDispatcher(IClientDispatcher):
    """Dispatch currently active client.

    Handles getting current client (LocalClient) and entering/exiting the stack for the client.
    """

    def __init__(self):
        self.client_stack = []

    @property
    def current_client(self) -> Optional[LocalClient]:
        """Get the currently active client."""
        if len(self.client_stack) == 0:
            raise errors.ConfigurationError("No client configured for injection")

        return self.client_stack[-1]

    def push_client_to_stack(
        self, path: Union[Path, str], renku_home: str = RENKU_HOME, external_storage_requested: bool = True
    ) -> None:
        """Create and push a new client to the stack."""
        new_client = LocalClient(path)
        self.client_stack.append(new_client)

    def push_created_client_to_stack(self, client: LocalClient) -> None:
        """Push an already created client to the stack."""

        self.client_stack.append(client)

    def pop_client(self) -> None:
        """Remove the current client from the stack."""
        self.client_stack.pop()
