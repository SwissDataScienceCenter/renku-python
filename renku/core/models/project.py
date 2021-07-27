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
from pathlib import Path
from typing import List
from urllib.parse import quote, urlparse

from marshmallow import EXCLUDE

from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION
from renku.core.metadata.database import persistent
from renku.core.models import projects as old_projects
from renku.core.models.calamus import DateTimeList, JsonLDSchema, Nested, StringList, fields, prov, renku, schema
from renku.core.models.provenance.agent import Person, PersonSchema
from renku.core.utils.datetime8601 import fix_timezone, local_now, parse_date


class Project(persistent.Persistent):
    """Represent a project."""

    def __init__(
        self,
        *,
        agent_version: str = None,
        automated_update: bool = False,
        creator: Person,
        date_created: datetime = None,
        id: str = None,
        immutable_template_files: List[str] = None,
        name: str,
        template_id: str = None,
        template_metadata: str = "{}",
        template_ref: str = None,
        template_source: str = None,
        template_version: str = None,
        version: str = str(SUPPORTED_PROJECT_VERSION),
    ):
        date_created = parse_date(date_created) or local_now()

        if not id:
            namespace, name = Project.get_namespace_and_name(name=name, creator=creator)
            id = Project.generate_id(namespace=namespace, name=name)

        self.agent_version: str = agent_version
        self.automated_update: bool = automated_update
        self.creator: Person = creator
        self.date_created: datetime = fix_timezone(date_created) or local_now()
        self.id: str = id
        self.immutable_template_files: List[str] = immutable_template_files
        self.name: str = name
        self.template_id: str = template_id
        self.template_metadata: str = template_metadata
        self.template_ref: str = template_ref
        self.template_source: str = template_source
        self.template_version: str = template_version
        self.version: str = version

    @classmethod
    def from_project(cls, project: old_projects.Project) -> "Project":
        """Create an instance from an old Project."""

        def convert_id(id):
            id_path = urlparse(id).path
            id_path = id_path.replace(f"/{old_projects.PROJECT_URL_PATH}/", "")
            id_path = Path(id_path)
            namespace, name = str(id_path.parent), id_path.name
            return cls.generate_id(namespace=namespace, name=name)

        return cls(
            agent_version=project.agent_version,
            automated_update=project.automated_update,
            creator=Person.from_person(project.creator),
            date_created=project.created,
            id=convert_id(project._id),
            immutable_template_files=project.immutable_template_files,
            name=project.name,
            template_id=project.template_id,
            template_metadata=project.template_metadata,
            template_ref=project.template_ref,
            template_source=project.template_source,
            template_version=project.template_version,
            version=project.version,
        )

    @classmethod
    def from_client(cls, client, name: str = None, creator: Person = None) -> "Project":
        """Create an instance from a LocalClient."""
        namespace, name = cls.get_namespace_and_name(client=client, name=name, creator=creator)
        creator = creator or Person.from_git(client.repo.git)

        if not name:
            raise ValueError("Project name not set")
        if not creator:
            raise ValueError("Project Creator not set")

        id = cls.generate_id(namespace=namespace, name=name)
        return cls(creator=creator, id=id, name=name)

    @staticmethod
    def get_namespace_and_name(*, client=None, name: str = None, creator: Person = None):
        """Return Project's namespace and name from various objects."""
        namespace = None

        if client:
            remote = client.remote
            namespace = remote.get("owner")
            name = remote.get("name") or name

            if not creator:
                if client.renku_metadata_path.exists():
                    commit = client.find_previous_commit(client.renku_metadata_path, return_first=True)
                    creator = Person.from_commit(commit)
                else:
                    # this assumes the project is being newly created
                    creator = Person.from_git(client.repo)

        if not namespace and creator:
            namespace = creator.email.split("@")[0]

        return namespace, name

    @staticmethod
    def generate_id(namespace: str, name: str):
        """Generate an id for Project."""
        assert namespace, "Cannot generate Project id with no namespace"
        assert name, "Cannot generate Project id with no name"

        namespace = quote(namespace.strip("/"), safe="/")
        name = quote(name, safe="")

        return f"/projects/{namespace}/{name}"


class ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = Project
        unknown = EXCLUDE

    agent_version = StringList(schema.agent, missing="pre-0.11.0")
    automated_update = fields.Boolean(renku.automatedTemplateUpdate, missing=False)
    creator = Nested(schema.creator, PersonSchema, missing=None)
    date_created = DateTimeList(schema.dateCreated, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    id = fields.Id(missing=None)
    immutable_template_files = fields.List(renku.immutableTemplateFiles, fields.String(), missing=[])
    name = fields.String(schema.name, missing=None)
    template_id = fields.String(renku.templateId, missing=None)
    template_metadata = fields.String(renku.templateMetadata, missing=None)
    template_ref = fields.String(renku.templateReference, missing=None)
    template_source = fields.String(renku.templateSource, missing=None)
    template_version = fields.String(renku.templateVersion, missing=None)
    version = StringList(schema.schemaVersion, missing="1")
