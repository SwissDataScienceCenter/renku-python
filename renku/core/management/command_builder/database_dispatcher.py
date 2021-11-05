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
"""Renku database dispatcher."""
from pathlib import Path
from typing import Union

from renku.core import errors
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.metadata.database import Database


class DatabaseDispatcher(IDatabaseDispatcher):
    """Interface for the DatabaseDispatcher.

    Handles getting current database (Database) and entering/exiting the stack for the database.
    """

    def __init__(self):
        self.database_stack = []

    @property
    def current_database(self) -> Database:
        """Get the currently active database."""
        if len(self.database_stack) == 0:
            raise errors.ConfigurationError("No database configured for injection")

        return self.database_stack[-1][0]

    def push_database_to_stack(self, path: Union[Path, str], commit: bool = False) -> None:
        """Create and push a new client to the stack."""
        new_database = Database.from_path(path)
        self.database_stack.append((new_database, commit))

    def pop_database(self) -> None:
        """Remove the current client from the stack."""
        popped_database = self.database_stack.pop()

        if popped_database[1]:
            popped_database[0].commit()

    def finalize_dispatcher(self) -> None:
        """Close all database contexts."""
        while self.database_stack:
            self.pop_database()
