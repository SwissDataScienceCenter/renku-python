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
"""Client for handling projects."""


class ProjectsApiMixin(object):
    """Client for handling projects."""

    def get_project(self, project_id):
        """Get existing project."""
        resp = self.get(
            self._url('/api/projects/{0}', project_id),
            expected_status_code=200
        )
        return resp.json()

    def list_projects(self):
        """Return an iterator for all projects."""
        resp = self.get(self._url('/api/projects'))
        return resp.json()['projects']

    def create_project(self, project):
        """Create a new project and register it on the knowledge graph."""
        resp = self.post(
            self._url('/api/projects'),
            json=project,
        )
        return resp.json()
