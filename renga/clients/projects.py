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


from . import knowledge_graph


class Project(object):
    """Renga platform project."""

    def __init__(self, name, vertex_id=None):
        """Initialize Project."""
        self.name = name
        self.vertex_id = vertex_id


class ProjectClient(object):
    """Client for handling projects."""

    def __init__(self, platform_endpoint):
        """Initialize Project client."""
        self.platform_endpoint = platform_endpoint
        self._KGCLient = None

    @property
    def KGClient(self):
        if self._KGCLient is None:
            self._KGCLient = knowledge_graph.KnowledgeGraphClient(
                self.platform_endpoint)
        return self._KGCLient

    def create_project(self, name):
        """Create a new project and register it on the knowledge graph."""
        project = Project(name)

        operation = self.KGClient.vertex_operation(
            project, temp_id=0, named_type='project:project')

        vertex_id = self.KGClient.mutation(
            [
                operation,
            ], wait_for_response=True)

        return Project(name=name, vertex_id=vertex_id)
