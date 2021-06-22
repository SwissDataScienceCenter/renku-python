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

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, wfprov
from renku.core.utils.git import get_object_hash


class Entity:
    """Represent a file."""

    def __init__(self, *, checksum: str, id: str = None, path: Union[Path, str]):
        assert id is None or isinstance(id, str)
        assert not os.path.isabs(path), f"Entity is being created with absolute path: '{path}'"

        self.checksum: str = checksum
        self.id: str = id or Entity.generate_id(checksum, path)
        self.path: str = str(path)

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
    def from_revision(
        cls, client, path: Union[Path, str], revision: str = None, find_previous: bool = True
    ) -> "Entity":
        """Return dependency from given path and revision."""
        revision = revision or "HEAD"

        if find_previous:
            revision = client.find_previous_commit(path, revision=revision)

        client, commit, path = client.resolve_in_submodules(revision, path)

        checksum = get_object_hash(repo=client.repo, revision=revision, path=path)
        # TODO: What if checksum is None
        assert checksum is not None, f"Entity not found: {revision}:{path}"
        # TODO: What would be checksum for a directory if it's not committed yet.
        id = cls.generate_id(checksum=checksum, path=path)

        absolute_path = client.path / path
        if str(path) != "." and absolute_path.is_dir():
            members = cls.get_directory_members(client, commit, absolute_path)
            entity = Collection(id=id, checksum=checksum, path=path, members=members)
        else:
            entity = cls(id=id, checksum=checksum, path=path)

        return entity

    @classmethod
    def get_directory_members(cls, client, commit, absolute_path: Path) -> List["Entity"]:
        """Return first-level files/directories in a directory."""
        files_in_commit = commit.stats.files
        members: List[Entity] = []

        for member in absolute_path.iterdir():
            if member.name == ".gitkeep":
                continue

            member_path = str(member.relative_to(client.path))
            find_previous = True

            if member_path in files_in_commit:
                # we already know the newest commit, no need to look it up
                find_previous = False

            try:
                assert all(member_path != m.path for m in members)

                members.append(cls.from_revision(client, member_path, commit, find_previous=find_previous))
            except KeyError:
                pass

        return members


class Collection(Entity):
    """Represent a directory with files."""

    def __init__(self, *, checksum: str, id: str = None, path: Union[Path, str], members: List[Entity] = None):
        super().__init__(id=id, checksum=checksum, path=path)
        self.members: List[Entity] = members or []


class NewEntitySchema(JsonLDSchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        # NOTE: wfprov.Artifact is not removed for compatibility with older project
        rdf_type = [prov.Entity, wfprov.Artifact]
        model = Entity

    checksum = fields.String(renku.checksum, missing=None)
    id = fields.Id()
    path = fields.String(prov.atLocation)


class NewCollectionSchema(NewEntitySchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Collection
        model = Collection

    members = Nested(prov.hadMember, [NewEntitySchema, "NewCollectionSchema"], many=True)
