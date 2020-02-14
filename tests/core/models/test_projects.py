# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
from datetime import datetime, timezone

import pytest
import yaml
from freezegun import freeze_time

from renku.core.models.jsonld import NoDatesSafeLoader, asjsonld
from renku.core.models.projects import Project
from renku.core.models.provenance.agents import Person

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

PROJECT_V2 = """
'@context':
  _id: '@id'
  created: schema:dateCreated
  creator: schema:creator
  name: schema:name
  prov: http://www.w3.org/ns/prov#
  schema: http://schema.org/
  updated: schema:dateUpdated
  version: schema:schemaVersion
'@type':
- prov:Location
- schema:Project
_id: https://renku.ch/demo/demo
created: '2019-09-18T15:18:16.866609+00:00'
creator:
  '@context':
    _id: '@id'
    affiliation: schema:affiliation
    alternate_name: schema:alternateName
    email: schema:email
    name: schema:name
    schema: http://schema.org/
  '@type': schema:Person
  _id: mailto:demo@example.com
  affiliation: null
  alternate_name: null
  email: demo@example.com
  name: demo user
name: demo
updated: '2019-09-18T15:18:16.868838+00:00'
version: 2
"""

PROJECT_V1_BROKEN = """
'@context':
  created: http://schema.org/dateCreated
  foaf: http://xmlns.com/foaf/0.1/
  name: notexisting:name
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
    keys = ['schema', 'created', 'creator', 'name', 'updated']
    assert all(k in Project._jsonld_context for k in keys)


def test_project_serialization():
    """Test project serialization with JSON-LD context."""
    with freeze_time('2017-03-01T08:00:00.000000+00:00') as frozen_time:
        project = Project(name='demo')
        assert project.name == 'demo'
        assert project.created == frozen_time().replace(tzinfo=timezone.utc)
        assert project.updated == frozen_time().replace(tzinfo=timezone.utc)

    data = asjsonld(project)
    assert 'schema:Project' in data['@type']
    assert 'prov:Location' in data['@type']

    context = data['@context']
    assert 'schema:name' == context['name']
    assert Person._jsonld_context == context['creator']['@context']
    assert 'schema:dateUpdated' == context['updated']
    assert 'schema:dateCreated' == context['created']
    assert 'schema:schemaVersion' == context['version']


@pytest.mark.parametrize(
    'project_meta,version,is_broken', [
        (PROJECT_V1, 1, False),
        (PROJECT_V1_BROKEN, 1, True),
        (PROJECT_V2, 2, False),
    ]
)
def test_project_metadata_compatibility(project_meta, version, is_broken):
    """Test loading of the initial version."""
    project = Project.from_jsonld(
        yaml.load(project_meta, Loader=NoDatesSafeLoader)
    )

    assert str(version) == project.version

    if not is_broken:
        assert 'demo' == project.name

    assert 'schema:name' == project._jsonld_context['name']
    main_context_creator = project._jsonld_context['creator']
    assert Person._jsonld_context == main_context_creator['@context']
    assert 'schema:dateUpdated' == project._jsonld_context['updated']
    assert 'schema:dateCreated' == project._jsonld_context['created']
    assert 'schema:schemaVersion' == project._jsonld_context['version']


@pytest.mark.parametrize('project_meta', [PROJECT_V1, PROJECT_V2])
def test_project_datetime_loading(project_meta):
    """Check that datetime is correctly loaded."""
    project = Project.from_jsonld(
        yaml.load(project_meta, Loader=NoDatesSafeLoader)
    )

    assert isinstance(project.updated, datetime)
    assert isinstance(project.created, datetime)

    assert project.updated.tzinfo is not None
    assert project.created.tzinfo is not None


def test_project_creator_deserialization(client, project):
    """Check that the correct creator is returned on deserialization."""
    from renku.core.models.provenance.agents import Person

    # modify the project metadata to change the creator
    project = client.project
    project.creator = Person(email='johndoe@example.com', name='Johnny Doe')
    project.to_yaml()
    client.repo.git.commit(
        '-a', '--amend', '-C', 'HEAD', '--author',
        'Johnny Doe <johndoe@example.com>', '--no-verify'
    )

    # the project creator should always be the one in the metadata
    assert 'johndoe@example.com' == client.project.creator.email
    assert 'Johnny Doe' == client.project.creator.name
    assert client.project.creator.label == client.project.creator.name

    # Remove the creator from metadata
    project = client.project
    project.creator = None
    project.to_yaml()
    client.repo.git.commit(
        '-a', '--amend', '-C', 'HEAD', '--author',
        'Jane Doe <janedoe@example.com>', '--no-verify'
    )

    # now the creator should be the one from the commit
    project = Project.from_yaml(client.renku_metadata_path, client=client)
    assert 'janedoe@example.com' == project.creator.email
    assert 'Jane Doe' == project.creator.name
    assert project.creator.label == project.creator.name
