# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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

import requests
from werkzeug.utils import cached_property

from renga.client._datastructures import AccessTokenMixin, Endpoint, \
    EndpointMixin

from ._datastructures import namedtuple

CreateProject = namedtuple(
    'CreateProject', ['name', 'labels'], default_values={'labels': None})
"""Renga platform project creation request."""

Project = namedtuple(
    'Project', ['identifier', 'name', 'labels'],
    default_values={'labels': None})
"""Renga platform project."""


class ProjectsClient(EndpointMixin, AccessTokenMixin):
    """Client for handling projects."""

    projects_url = Endpoint('/')
    project_url = Endpoint('/{project_id}')

    def __init__(self, endpoint, access_token):
        """Create a storage client."""
        EndpointMixin.__init__(self, endpoint)
        AccessTokenMixin.__init__(self, access_token)

    def __getitem__(self, project_id):
        """Get existing project."""
        resp = requests.get(
            self.project_url.format(project_id=project_id),
            headers=self.headers, )
        if resp.status == 200:
            return Project(**resp.json())
        elif resp.status == 400:
            raise ValueError(project_id)
        elif resp.status == 404:
            raise KeyError(project_id)
        raise RuntimeError(resp.status)

    def __iter__(self):
        """Return an iterator for all projects."""
        resp = requests.get(
            self.projects_url,
            headers=self.headers, )
        return (Project(**project) for project in resp.json()['projects'])

    def create(self, project):
        """Create a new project and register it on the knowledge graph."""
        resp = requests.post(
            self.projects_url,
            headers=self.headers,
            json=project, )
        return Project(**resp.json())
