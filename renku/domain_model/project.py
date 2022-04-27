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
"""Project class."""

from datetime import datetime
from typing import Dict, List, Optional, cast
from urllib.parse import quote

import persistent

from renku.core import errors
from renku.core.util.datetime8601 import fix_datetime, local_now, parse_date
from renku.core.util.git import get_git_user
from renku.core.util.os import normalize_to_ascii
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.version import __minimum_project_version__


class Project(persistent.Persistent):
    """Represent a project."""

    keywords: List[str] = list()

    # NOTE: the minimum version of renku to needed to work with a project
    # This should be bumped on metadata version changes and when we do not forward-compatible on-the-fly migrations
    minimum_renku_version: str = __minimum_project_version__

    def __init__(
        self,
        *,
        agent_version: Optional[str] = None,
        annotations: Optional[List[Annotation]] = None,
        automated_update: bool = False,
        creator: Person,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        immutable_template_files: Optional[List[str]] = None,
        name: Optional[str] = None,
        template_id: Optional[str] = None,
        template_metadata: str = "{}",
        template_ref: Optional[str] = None,
        template_source: Optional[str] = None,
        template_version: Optional[str] = None,
        version: Optional[str] = None,
        keywords: Optional[List[str]] = None,
    ):
        from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION

        version = cast(str, version or SUPPORTED_PROJECT_VERSION)
        date_created = parse_date(date_created) or local_now()

        if id is None:
            namespace, generated_name = Project.get_namespace_and_name(name=name, creator=creator)
            assert generated_name is not None, "Cannot generate Project id with no name"
            id = Project.generate_id(namespace=namespace, name=generated_name)

        self.agent_version: Optional[str] = agent_version
        self.annotations: List[Annotation] = annotations or []
        self.automated_update: bool = automated_update
        self.creator: Person = creator
        self.date_created: datetime = fix_datetime(date_created) or local_now()
        self.description: Optional[str] = description
        self.id: str = id
        self.immutable_template_files: Optional[List[str]] = immutable_template_files
        self.name: Optional[str] = name
        self.template_id: Optional[str] = template_id
        self.template_metadata: str = template_metadata
        self.template_ref: Optional[str] = template_ref
        self.template_source: Optional[str] = template_source
        self.template_version: Optional[str] = template_version
        self.version: str = version
        self.keywords = keywords or []

        # NOTE: We copy this over as class variables don't get saved in the DB
        self.minimum_renku_version = Project.minimum_renku_version

    @classmethod
    def from_client(
        cls,
        client,
        name: Optional[str] = None,
        description: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        custom_metadata: Optional[Dict] = None,
        creator: Optional[Person] = None,
    ) -> "Project":
        """Create an instance from a LocalClient."""
        namespace, name = cls.get_namespace_and_name(client=client, name=name, creator=creator)
        creator = creator or get_git_user(client.repository)
        annotations = None

        if custom_metadata:
            annotations = [Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku")]

        if creator is None:
            raise ValueError("Project Creator not set")

        if name is None:
            raise ValueError("Project 'name' not set and could not be generated")

        id = cls.generate_id(namespace=namespace, name=name)
        return cls(
            creator=creator, id=id, name=name, description=description, keywords=keywords, annotations=annotations
        )

    @staticmethod
    def get_namespace_and_name(*, client=None, name: Optional[str] = None, creator: Optional[Person] = None):
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
