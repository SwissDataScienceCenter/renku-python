# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION
from renku.core.models import jsonld
from renku.core.models.datastructures import Collection
from renku.core.models.provenance.agents import Person
from renku.core.utils.datetime8601 import parse_date

PROJECT_URL_PATH = 'projects'


@jsonld.s(
    type=[
        'schema:Project',
        'prov:Location',
    ],
    context={
        'schema': 'http://schema.org/',
        'prov': 'http://www.w3.org/ns/prov#'
    },
    translate={
        'http://schema.org/name': 'http://xmlns.com/foaf/0.1/name',
        'http://schema.org/Project': 'http://xmlns.com/foaf/0.1/Project'
    },
    slots=True,
)
class Project(object):
    """Represent a project."""

    name = jsonld.ib(default=None, context='schema:name')

    created = jsonld.ib(
        converter=parse_date,
        context='schema:dateCreated',
    )

    updated = jsonld.ib(
        converter=parse_date,
        context='schema:dateUpdated',
    )

    version = jsonld.ib(
        converter=str,
        default=str(SUPPORTED_PROJECT_VERSION),
        context='schema:schemaVersion',
    )

    client = attr.ib(default=None, kw_only=True)

    creator = jsonld.ib(
        default=None,
        kw_only=True,
        context={
            '@id': 'schema:creator',
        },
        type=Person
    )

    _id = jsonld.ib(context='@id', kw_only=True, default=None)

    @created.default
    @updated.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        if not self.creator and self.client:
            if self.client.renku_metadata_path.exists():
                self.creator = Person.from_commit(
                    self.client.find_previous_commit(
                        self.client.renku_metadata_path, return_first=True
                    ),
                )
            else:
                # this assumes the project is being newly created
                self.creator = Person.from_git(self.client.repo)

        try:
            self._id = self.project_id
        except ValueError:
            """Fallback to old behaviour."""
            if not self._id and self.client and self.client.project:
                self._id = self.client.project._id

    @property
    def project_id(self):
        """Return the id for the project based on the repo origin remote."""
        import pathlib
        import urllib

        # Determine the hostname for the resource URIs.
        # If RENKU_DOMAIN is set, it overrides the host from remote.
        # Default is localhost.
        host = 'localhost'

        if not self.creator:
            raise ValueError('Project Creator not set')

        owner = self.creator.email.split('@')[0]
        name = self.name

        if self.client:
            remote = self.client.remote
            host = self.client.remote.get('host') or host
            owner = remote.get('owner') or owner
            name = remote.get('name') or name
        host = os.environ.get('RENKU_DOMAIN') or host
        if name:
            name = urllib.parse.quote(name, safe='')
        else:
            raise ValueError('Project name not set')

        project_url = urllib.parse.urljoin(
            'https://{host}'.format(host=host),
            pathlib.posixpath.join(PROJECT_URL_PATH, owner, name or 'NULL')
        )
        return project_url


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
        data = self._client.api.create_project({'name': name})
        return self.Meta.model(data, client=self._client, collection=self)

    def __getitem__(self, project_id):
        """Get an existing project by its id."""
        return self.Meta.model(
            self._client.api.get_project(project_id),
            client=self._client,
            collection=self
        )

    def __iter__(self):
        """Return all projects."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_projects()
        )
