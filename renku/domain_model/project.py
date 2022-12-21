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

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, cast
from urllib.parse import quote

import persistent

from renku.core import errors
from renku.core.util.datetime8601 import fix_datetime, local_now, parse_date
from renku.core.util.git import get_git_user
from renku.core.util.os import normalize_to_ascii
from renku.core.util.util import NO_VALUE
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.version import __minimum_project_version__

if TYPE_CHECKING:
    from renku.domain_model.project_context import ProjectContext, ProjectRemote
    from renku.infrastructure.repository import Repository


@dataclass
class ProjectTemplateMetadata:
    """Metadata about the template used in a project."""

    template_id: Optional[str] = None
    metadata: str = ""
    template_ref: Optional[str] = None
    template_source: Optional[str] = None
    template_version: Optional[str] = None
    immutable_template_files: Optional[List[str]] = None


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
        creator: Person,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        template_metadata: Optional[ProjectTemplateMetadata] = None,
        version: Optional[str] = None,
        keywords: Optional[List[str]] = None,
    ):
        from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION

        version = cast(str, version or SUPPORTED_PROJECT_VERSION)
        date_created = parse_date(date_created) or local_now()

        if id is None:
            namespace, generated_name = Project.get_namespace_and_name(name=name, creator=creator)
            assert generated_name is not None, "Cannot generate Project id with no name"
            id = Project.generate_id(namespace=namespace, name=generated_name)

        self.name: Optional[str] = name
        self.agent_version: Optional[str] = agent_version
        self.annotations: List[Annotation] = annotations or []
        self.creator: Person = creator
        self.date_created: datetime = fix_datetime(date_created) or local_now()
        self.description: Optional[str] = description
        self.id: str = id
        self.version: str = version
        self.keywords = keywords or []

        self.template_metadata: ProjectTemplateMetadata = template_metadata or ProjectTemplateMetadata()

        # NOTE: We copy this over as class variables don't get saved in the DB
        self.minimum_renku_version = Project.minimum_renku_version

    @classmethod
    def from_project_context(
        cls,
        project_context: "ProjectContext",
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        description: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        custom_metadata: Optional[Dict] = None,
        creator: Optional[Person] = None,
    ) -> "Project":
        """Create an instance from a path.

        Args:
            cls: The class.
            name(Optional[str]): Name of the project (when creating a new one) (Default value = None).
            namespace(Optional[str]): Namespace of the project (when creating a new one) (Default value = None).
            description(Optional[str]): Project description (when creating a new one) (Default value = None).
            keywords(Optional[List[str]]): Keywords for the project (when creating a new one) (Default value = None).
            custom_metadata(Optional[Dict]): Custom JSON-LD metadata (when creating a new project)
                (Default value = None).
            creator(Optional[Person]): The project creator.
        """
        creator = creator or get_git_user(repository=project_context.repository)

        namespace, name = cls.get_namespace_and_name(
            remote=project_context.remote, name=name, namespace=namespace, creator=creator
        )
        annotations = None

        if custom_metadata:
            annotations = [Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku")]

        if creator is None:
            raise errors.ParameterError("Project Creator not set", "creator")

        if name is None:
            raise errors.ParameterError("Project 'name' not set and could not be generated", "name")

        if namespace is None:
            raise errors.ParameterError("Project 'namespace' not set and could not be generated", "namespace")

        id = cls.generate_id(namespace=namespace, name=name)
        return cls(
            creator=creator, id=id, name=name, description=description, keywords=keywords, annotations=annotations
        )

    @staticmethod
    def get_namespace_and_name(
        *,
        remote: Optional["ProjectRemote"] = None,
        repository: Optional["Repository"] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        creator: Optional[Person] = None,
    ):
        """Return Project's namespace and name from various objects."""
        if remote:
            namespace = namespace or remote.owner
            name = name or remote.name

        if not creator and repository:
            creator = get_git_user(repository=repository)

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

    def update_metadata(self, custom_metadata=None, custom_metadata_source=None, **kwargs):
        """Updates metadata."""
        editable_attributes = ["creator", "description", "keywords"]
        for name, value in kwargs.items():
            if name not in editable_attributes:
                raise errors.ParameterError(f"Cannot edit field: '{name}'")
            if value is not NO_VALUE and value != getattr(self, name):
                setattr(self, name, value)

        if custom_metadata is not NO_VALUE and custom_metadata_source is not NO_VALUE:
            existing_metadata = [a for a in self.annotations if a.source != custom_metadata_source]
            if custom_metadata is not None:
                if not isinstance(custom_metadata, list):
                    custom_metadata = [custom_metadata]
                for icustom_metadata in custom_metadata:
                    existing_metadata.append(
                        Annotation(
                            id=Annotation.generate_id(),
                            body=icustom_metadata,
                            source=custom_metadata_source,
                        )
                    )

            self.annotations = existing_metadata
