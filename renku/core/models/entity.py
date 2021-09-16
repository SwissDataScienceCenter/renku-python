# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
from typing import List, Union
from urllib.parse import quote

from renku.core.metadata.immutable import Immutable
from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku
from renku.core.utils.git import get_object_hash

_entity_cache = {}


class Entity(Immutable):
    """Represent a file."""

    __slots__ = ("checksum", "path")

    def __init__(self, *, checksum: str, id: str = None, path: Union[Path, str], **kwargs):
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

    @staticmethod
    def generate_id(checksum: str, path: Union[Path, str]) -> str:
        """Generate an Entity identifier."""
        quoted_path = quote(str(path).strip("/"))

        return f"/entities/{checksum}/{quoted_path}"

    @classmethod
    def from_revision(cls, client, path: Union[Path, str], revision: str = None) -> "Entity":
        """Return dependency from given path and revision."""
        revision = revision or "HEAD"
        assert isinstance(revision, str), f"Invalid revision: {revision}"

        # FIXME: Implement the cache properly: It's mainly used in migrations
        global _entity_cache
        key = (revision, str(path))
        cached_entry = _entity_cache.get(key)
        if cached_entry:
            return cached_entry

        # TODO: What checksum we get at "HEAD" if object is staged but not committed
        checksum = get_object_hash(repo=client.repo, revision=revision, path=path)
        # NOTE: If object was not found at a revision it's either removed or exists in a different revision; keep the
        # entity and use revision as checksum
        checksum = checksum or revision
        # TODO: What would be checksum for a directory if it's not committed yet.
        id = cls.generate_id(checksum=checksum, path=path)

        absolute_path = client.path / path
        if str(path) != "." and absolute_path.is_dir():
            members = cls.get_directory_members(client, revision, absolute_path)
            entity = Collection(id=id, checksum=checksum, path=path, members=members)
        else:
            entity = cls(id=id, checksum=checksum, path=path)

        _entity_cache[cached_entry] = entity

        return entity

    @classmethod
    def get_directory_members(cls, client, revision, absolute_path: Path) -> List["Entity"]:
        """Return first-level files/directories in a directory."""
        members: List[Entity] = []

        for member in absolute_path.iterdir():
            if member.name == ".gitkeep":
                continue

            member_path = str(member.relative_to(client.path))

            try:
                assert all(member_path != m.path for m in members)

                entity = cls.from_revision(client, member_path, revision)
                # NOTE: If a path is not found at a revision we assume that it didn't exist at that revision
                if entity:
                    members.append(entity)
            except KeyError:
                pass

        return members


class Collection(Entity):
    """Represent a directory with files."""

    __slots__ = ("members",)

    def __init__(self, *, checksum: str, id: str = None, path: Union[Path, str], members: List[Entity] = None):
        members = tuple(members) if members else ()
        super().__init__(checksum=checksum, id=id, path=path, members=members)


class EntitySchema(JsonLDSchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity]
        model = Entity

    checksum = fields.String(renku.checksum, missing=None)
    id = fields.Id()
    path = fields.String(prov.atLocation)


class CollectionSchema(EntitySchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Collection
        model = Collection

    members = Nested(prov.hadMember, [EntitySchema, "CollectionSchema"], many=True)
