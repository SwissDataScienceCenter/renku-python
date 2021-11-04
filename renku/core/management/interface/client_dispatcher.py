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
"""Renku client dispatcher interface."""

from abc import ABC
from pathlib import Path
from typing import Union


class IClientDispatcher(ABC):
    """Interface for the ClientDispatcher.

    Handles getting current client (LocalClient) and entering/exiting the stack for the client.
    """

    @property
    def current_client(self):
        """Get the currently active client."""
        raise NotImplementedError

    def push_client_to_stack(
        self, path: Union[Path, str], renku_home: str = ".renku", external_storage_requested: bool = True
    ) -> None:
        """Create and push a new client to the stack."""
        raise NotImplementedError

    def push_created_client_to_stack(self, client) -> None:
        """Push an already created client to the stack."""
        raise NotImplementedError

    def pop_client(self) -> None:
        """Remove the current client from the stack."""
        raise NotImplementedError
