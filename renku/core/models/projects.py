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
"""Model objects representing projects."""

import datetime
import os

import attr
from marshmallow import EXCLUDE
from marshmallow.decorators import pre_dump

from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION
from renku.core.models import jsonld
from renku.core.models.calamus import DateTimeList, JsonLDSchema, Nested, StringList, fields, prov, renku, schema
from renku.core.models.datastructures import Collection
from renku.core.models.provenance.agents import Person, PersonSchema
from renku.core.utils.datetime8601 import parse_date

PROJECT_URL_PATH = "projects"


@attr.s(slots=True,)
class Project:
    """Represent a project."""

    name = attr.ib(default=None,)

    created = attr.ib(converter=parse_date,)

    version = attr.ib(converter=str, default=str(SUPPORTED_PROJECT_VERSION),)

    agent_version = attr.ib(converter=str, default="pre-0.11.0")

    template_source = attr.ib(type=str, default=None)

    template_ref = attr.ib(type=str, default=None)

    template_id = attr.ib(type=str, default=None)

    template_version = attr.ib(type=str, default=None)

    template_metadata = attr.ib(type=str, default="{}")

    immutable_template_files = attr.ib(factory=list)

    automated_update = attr.ib(converter=bool, default=False)

    client = attr.ib(default=None, kw_only=True)

    creator = attr.ib(default=None, kw_only=True)

    _id = attr.ib(kw_only=True, default=None)

    _metadata_path = attr.ib(default=None, init=False)

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        if not self.creator and self.client:
            if self.client.renku_metadata_path.exists():
                self.creator = Person.from_commit(
                    self.client.find_previous_commit(self.client.renku_metadata_path, return_first=True),
                )
            else:
                # this assumes the project is being newly created
                self.creator = Person.from_git(self.client.repo)

        try:
            self._id = self.project_id
        except ValueError:
            """Fallback to old behaviour."""
            if self._id:
                pass
            elif self.client and self.client.is_project_set():
                self._id = self.client.project._id
            else:
                raise

    @property
    def project_id(self):
        """Return the id for the project."""
        return generate_project_id(client=self.client, name=self.name, creator=self.creator)

    @classmethod
    def from_yaml(cls, path, client=None):
        """Return an instance from a YAML file."""
        data = jsonld.read_yaml(path)
        self = cls.from_jsonld(data=data, client=client)
        self._metadata_path = path

        return self

    @classmethod
    def from_jsonld(cls, data, client=None):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return ProjectSchema(client=client).load(data)

    def to_yaml(self, path=None):
        """Write an instance to the referenced YAML file."""
        from renku import __version__

        self.agent_version = __version__

        self._metadata_path = path or self._metadata_path
        data = ProjectSchema().dump(self)
        jsonld.write_yaml(path=self._metadata_path, data=data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return ProjectSchema().dump(self)


class ProjectCollection(Collection):
    """Represent projects on the server.

    **Example**

    Create a project and check its name.

    # >>> project = client.projects.create(name='test-project')
    # >>> project.name
    # 'test-project'

    """

    class Meta:
        """Information about individual projects."""

        model = Project

    def create(self, name=None, **kwargs):
        """Create a new project.

        :param name: The name of the project.
        :returns: An instance of the newly create project.
        :rtype: renku.core.models.projects.Project
        """
        data = self._client.api.create_project({"name": name})
        return self.Meta.model(data, client=self._client, collection=self)

    def __getitem__(self, project_id):
        """Get an existing project by its id."""
        return self.Meta.model(self._client.api.get_project(project_id), client=self._client, collection=self)

    def __iter__(self):
        """Return all projects."""
        return (
            self.Meta.model(data, client=self._client, collection=self) for data in self._client.api.list_projects()
        )


class ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = Project
        unknown = EXCLUDE

    name = fields.String(schema.name, missing=None)
    created = DateTimeList(schema.dateCreated, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    version = StringList(schema.schemaVersion, missing="1")
    agent_version = fields.String(schema.agent, missing="pre-0.11.0")
    template_source = fields.String(renku.templateSource, missing=None)
    template_ref = fields.String(renku.templateReference, missing=None)
    template_id = fields.String(renku.templateId, missing=None)
    template_version = fields.String(renku.templateVersion, missing=None)
    template_metadata = fields.String(renku.templateMetadata, missing=None)
    immutable_template_files = fields.List(renku.immutableTemplateFiles, fields.String(), missing=[])
    automated_update = fields.Boolean(renku.automatedTemplateUpdate, missing=False)
    creator = Nested(schema.creator, PersonSchema, missing=None)
    _id = fields.Id(init_name="id", missing=None)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.created = self._fix_timezone(obj.created)
        return obj


def generate_project_id(client, name, creator):
    """Return the id for the project based on the repo origin remote."""
    import pathlib
    import urllib

    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"

    if not creator:
        raise ValueError("Project Creator not set")

    owner = creator.email.split("@")[0]

    if client:
        remote = client.remote
        host = client.remote.get("host") or host
        owner = remote.get("owner") or owner
        name = remote.get("name") or name
    host = os.environ.get("RENKU_DOMAIN") or host
    if name:
        name = urllib.parse.quote(name, safe="")
    else:
        raise ValueError("Project name not set")

    project_url = urllib.parse.urljoin(f"https://{host}", pathlib.posixpath.join(PROJECT_URL_PATH, owner, name))
    return project_url
