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
"""Tests for the knowledge graph client."""

from renga.clients import knowledge_graph, projects


def test_knowledge_graph_init(kg_requests):
    """Test knowldge graph client initialization."""
    KGClient = knowledge_graph.KnowledgeGraphClient('http://localhost/api')
    assert KGClient.named_types


def test_knowledge_graph_deploy_context(kg_requests):
    """Test sending a deployment context to the KG."""
    KGClient = knowledge_graph.KnowledgeGraphClient('http://localhost/api')

    class Dummy(object):
        pass

    context = Dummy()
    context.id = 1234
    context.spec = {'image': 'hello-world', 'ports': '9999'}
    operation = KGClient.vertex_operation(context, 0, 'deployer:context')
    assert len(operation['element']['properties']) == 3

    vertex_id = KGClient.mutation(
        [
            operation,
        ], wait_for_response=True)

    assert vertex_id == 1234


def test_knowledge_graph_deploy_execution(kg_requests):
    """Test sending a deployment context to the KG."""
    KGClient = knowledge_graph.KnowledgeGraphClient('http://localhost/api')

    class Dummy(object):
        pass

    execution = Dummy()
    execution.id = 1234
    execution.engine = 'docker'
    execution.namespace = 'default'
    operation = KGClient.vertex_operation(execution, 0, 'deployer:execution')
    assert len(operation['element']['properties']) == 3

    vertex_id = KGClient.mutation(
        [
            operation,
        ], wait_for_response=True)
    assert vertex_id == 1234


def test_knowledge_graph_add_project(kg_requests):
    """Test sending a deployment context to the KG."""
    KGClient = knowledge_graph.KnowledgeGraphClient('http://localhost/api')

    class Dummy(object):
        pass

    project = Dummy()
    project.name = 'MyProject'
    operation = KGClient.vertex_operation(project, 0, 'project:project')
    assert len(operation['element']['properties']) == 1

    vertex_id = KGClient.mutation(
        [
            operation,
        ], wait_for_response=True)
    assert vertex_id == 1234


def test_knowledge_graph_add_project(kg_requests):
    """Test sending a deployment context to the KG."""
    ProjectClient = projects.ProjectClient('http://localhost/api')

    project = ProjectClient.create_project(name='My Project')

    assert project.name == 'My Project'
    assert project.vertex_id == 1234
