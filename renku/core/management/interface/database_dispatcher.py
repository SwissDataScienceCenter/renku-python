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
"""Renku database dispatcher interface."""

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from renku.core.metadata.database import Database


class IDatabaseDispatcher(ABC):
    """Interface for the DatabaseDispatcher.

    Handles getting current database (Database) and entering/exiting the stack for the database.
    """

    @property
    def current_database(self) -> "Database":
        """Get the currently active database."""
        raise NotImplementedError

    def push_database_to_stack(self, path: Union[Path, str], commit: bool = False) -> None:
        """Create and push a new database to the stack."""
        raise NotImplementedError

    def pop_database(self) -> None:
        """Remove the current database from the stack."""
        raise NotImplementedError

    def finalize_dispatcher(self) -> None:
        """Close all database contexts."""
        raise NotImplementedError
