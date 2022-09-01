# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Represent provenance entities."""

import os.path
from pathlib import Path
from typing import List, Optional, Sequence, Union
from urllib.parse import quote

from renku.infrastructure.immutable import Immutable

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40


class Entity(Immutable):
    """Represent a file."""

    __slots__ = ("checksum", "path")

    path: str
    checksum: str

    def __init__(self, *, checksum: str, id: Optional[str] = None, path: Union[Path, str], **kwargs):
        path = str(path)

        assert id is None or isinstance(id, str)
        assert not os.path.isabs(path), f"Entity is being created with absolute path: '{path}'"

        id = id or Entity.generate_id(checksum, path)
        super().__init__(checksum=checksum, id=id, path=path, **kwargs)

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Entity):
            return False
        return self.checksum == other.checksum and self.path == other.path

    def __hash__(self):
        return hash((self.checksum, self.path))

    def __repr__(self) -> str:
        return f"{self.path}#{self.checksum}"

    @staticmethod
    def generate_id(checksum: str, path: Union[Path, str]) -> str:
        """Generate an Entity identifier."""
        quoted_path = quote(str(path).strip("/"))

        return f"/entities/{checksum}/{quoted_path}"


class Collection(Entity):
    """Represent a directory with files."""

    __slots__ = ("members",)

    members: Sequence[Entity]

    def __init__(
        self, *, checksum: str, id: Optional[str] = None, path: Union[Path, str], members: Optional[List[Entity]] = None
    ):
        members_tuple = tuple(members) if members else ()
        super().__init__(checksum=checksum, id=id, path=path, members=members_tuple)
