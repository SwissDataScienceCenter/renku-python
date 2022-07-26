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
"""Renku database gateway interface."""

from abc import ABC
from typing import TYPE_CHECKING, Generator

if TYPE_CHECKING:
    from persistent import Persistent


class IDatabaseGateway(ABC):
    """Gateway interface for basic database operations."""

    def initialize(self) -> None:
        """Initialize the database."""
        raise NotImplementedError

    def commit(self) -> None:
        """Commit changes to database."""
        raise NotImplementedError

    def get_modified_objects_from_revision(self, revision_or_range: str) -> Generator["Persistent", None, None]:
        """Get all database objects modified in a revision."""
        raise NotImplementedError
