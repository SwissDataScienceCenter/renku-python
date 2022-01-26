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
"""Project class."""

from datetime import datetime
from typing import Dict, List
from urllib.parse import quote

from renku.core import errors
from renku.core.metadata.database import persistent
from renku.core.models.provenance.agent import Person
from renku.core.models.provenance.annotation import Annotation
from renku.core.utils.datetime8601 import fix_datetime, local_now, parse_date
from renku.core.utils.git import get_git_user
from renku.core.utils.os import normalize_to_ascii


class Project(persistent.Persistent):
    """Represent a project."""

    keywords = None

    def __init__(
        self,
        *,
        agent_version: str = None,
        annotations: List[Annotation] = None,
        automated_update: bool = False,
        creator: Person,
        date_created: datetime = None,
        description: str = None,
        id: str = None,
        immutable_template_files: List[str] = None,
        name: str,
        template_id: str = None,
        template_metadata: str = "{}",
        template_ref: str = None,
        template_source: str = None,
        template_version: str = None,
        version: str = None,
        keywords: List[str] = None,
    ):
        from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION

        version = version or SUPPORTED_PROJECT_VERSION
        date_created = parse_date(date_created) or local_now()

        if not id:
            namespace, name = Project.get_namespace_and_name(name=name, creator=creator)
            id = Project.generate_id(namespace=namespace, name=name)

        self.agent_version: str = agent_version
        self.annotations: List[Annotation] = annotations or []
        self.automated_update: bool = automated_update
        self.creator: Person = creator
        self.date_created: datetime = fix_datetime(date_created) or local_now()
        self.description: str = description
        self.id: str = id
        self.immutable_template_files: List[str] = immutable_template_files
        self.name: str = name
        self.template_id: str = template_id
        self.template_metadata: str = template_metadata
        self.template_ref: str = template_ref
        self.template_source: str = template_source
        self.template_version: str = template_version
        self.version: str = version
        self.keywords: List[str] = keywords or []

    @classmethod
    def from_client(
        cls,
        client,
        name: str = None,
        description: str = None,
        keywords: List[str] = None,
        custom_metadata: Dict = None,
        creator: Person = None,
    ) -> "Project":
        """Create an instance from a LocalClient."""
        namespace, name = cls.get_namespace_and_name(client=client, name=name, creator=creator)
        creator = creator or get_git_user(client.repository)
        annotations = None

        if custom_metadata:
            annotations = [Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku")]

        if not creator:
            raise ValueError("Project Creator not set")

        id = cls.generate_id(namespace=namespace, name=name)
        return cls(
            creator=creator, id=id, name=name, description=description, keywords=keywords, annotations=annotations
        )

    @staticmethod
    def get_namespace_and_name(*, client=None, name: str = None, creator: Person = None):
        """Return Project's namespace and name from various objects."""
        namespace = None

        if client:
            remote = client.remote
            namespace = remote.get("owner")
            name = remote.get("name") or name

            if not creator:
                creator = get_git_user(client.repository)

        if not namespace and creator:
            namespace = creator.email.split("@")[0]

        return namespace, name

    @staticmethod
    def generate_id(namespace: str, name: str):
        """Generate an id for Project."""
        assert namespace, "Cannot generate Project id with no namespace"
        assert name, "Cannot generate Project id with no name"

        namespace = quote(namespace.strip("/"), safe="/")
        slug = normalize_to_ascii(name)

        return f"/projects/{namespace}/{slug}"

    def update_metadata(self, custom_metadata=None, **kwargs):
        """Updates metadata."""
        editable_attributes = ["creator", "description", "keywords"]
        for name, value in kwargs.items():
            if name not in editable_attributes:
                raise errors.ParameterError(f"Cannot edit field: '{name}'")
            if value and value != getattr(self, name):
                setattr(self, name, value)

        if custom_metadata:
            existing_metadata = [a for a in self.annotations if a.source != "renku"]

            existing_metadata.append(Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku"))

            self.annotations = existing_metadata
