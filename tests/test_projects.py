# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Test projects API."""

import yaml
from freezegun import freeze_time

from renku.models._jsonld import asjsonld
from renku.models.projects import Project

# Do not modify the content so we can ensure backwards compatibility.
PROJECT_V1 = """
'@context':
  created: http://schema.org/dateCreated
  foaf: http://xmlns.com/foaf/0.1/
  name: foaf:name
  updated: http://schema.org/dateUpdated
  version: http://schema.org/schemaVersion
'@type': foaf:Project
created: 2018-02-22 10:40:14.878009
name: demo
updated: 2018-02-22 15:33:11.089204
version: '1'
"""


def test_project_context():
    """Test project context definition."""
    assert 'foaf' in Project._jsonld_context
    assert 'created' in Project._jsonld_context


def test_project_serialization():
    """Test project serialization with JSON-LD context."""
    with freeze_time('2017-03-01 08:00:00') as frozen_time:
        project = Project(name='demo')
        assert project.name == 'demo'
        assert project.created == frozen_time()
        assert project.updated == frozen_time()

    data = asjsonld(project)
    assert data['@type'].endswith('Project')

    context = data['@context']
    assert 'created' in context


def test_project_metadata_compatibility():
    """Test loading of the initial version."""
    project = Project.from_jsonld(yaml.safe_load(PROJECT_V1))

    assert project.name == 'demo'
    assert project.version == '1'
