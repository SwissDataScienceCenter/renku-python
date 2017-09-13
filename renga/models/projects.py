# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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

from ._datastructures import Collection, Model


class Project(Model):
    """Represent a project."""

    @property
    def name(self):
        """The name of the project."""
        return self._response.get('name')


class ProjectsCollection(Collection):
    """Represent projects on the server."""

    class Meta:
        """Information about individual projects."""

        model = Project

    def create(self, name=None, **kwargs):
        """Create new project."""
        data = self._client.api.create_project({'name': name})
        return self.Meta.model(data, client=self._client, collection=self)

    def __getitem__(self, project_id):
        """Get existing project by its id."""
        return self.Meta.model(
            self._client.api.get_project(project_id),
            client=self._client,
            collection=self)

    def __iter__(self):
        """Return all projects."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_projects()
        )
