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
"""Migration models V10."""

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, cast
from urllib.parse import quote

import persistent

from renku.core.util.datetime8601 import fix_datetime, local_now, parse_date
from renku.core.util.git import get_git_user
from renku.core.util.os import normalize_to_ascii
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.version import __minimum_project_version__

if TYPE_CHECKING:
    from renku.domain_model.project_context import ProjectRemote
    from renku.infrastructure.repository import Repository


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
        from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION

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
