# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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

from . import _jsonld as jsonld
from ._datastructures import Collection


@jsonld.s(
    type='foaf:Project',
    context={
        'foaf': 'http://xmlns.com/foaf/0.1/',
    },
    # TODO show a working example
    # translate={
    #     'http://xmlns.com/foaf/0.1/name': 'http://schema.org/description',
    # },
    slots=True,
)
class Project(object):
    """Represent a project."""

    name = jsonld.ib(default=None, context='foaf:name')
    created = jsonld.ib(context='http://schema.org/dateCreated', )
    updated = jsonld.ib(context='http://schema.org/dateUpdated', )
    version = jsonld.ib(
        converter=str,
        default='1',
        context='http://schema.org/schemaVersion',
    )

    @created.default
    @updated.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()


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
        :rtype: renku.models.projects.Project
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
