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


from werkzeug.utils import cached_property

from ._datastructures import namedtuple
from .graph.mutation import GraphMutationClient

Project = namedtuple('Project', ['name', 'vertex_id'])
"""Renga platform project."""


class ProjectClient(object):
    """Client for handling projects."""

    def __init__(self, platform_endpoint):
        """Initialize Project client."""
        self.platform_endpoint = platform_endpoint

    @cached_property
    def _api(self):
        """Return a graph mutation client."""
        return GraphMutationClient(self.platform_endpoint)

    def create(self, name):
        """Create a new project and register it on the knowledge graph."""
        project = Project(name=name)
        # Create new node
        operation = self._api.vertex_operation(
            project, temp_id=0, named_type='project:project')
        # Get vertex id of the newly created project
        return project._replace(vertex_id=self._api.mutation(
            [operation], wait_for_response=True))
