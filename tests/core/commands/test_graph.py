# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Graph building tests."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from BTrees import OOBTree
from deepdiff import DeepDiff
from rdflib import Graph
from zc.relation.catalog import Catalog

from renku.command.graph import get_graph_for_all_objects, get_graph_for_revision, update_nested_node_host
from renku.command.view_model.graph import GraphViewModel
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.domain_model.dataset import Dataset, DatasetFile, DatasetTag, Url
from renku.domain_model.entity import Entity
from renku.domain_model.project import Project
from renku.domain_model.provenance.activity import Activity, Association
from renku.domain_model.provenance.agent import Person
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.plan import Plan
from renku.infrastructure.database import Index


@pytest.fixture()
def input_objects(request, dataset_model, activity_model, plan_model):
    """Use fixtures as input for parametrize.

    Since we can't use fixtures directly, we specify them with a placeholder and `indirect` calls them here.
    """
    if request.param == "dataset":
        return [dataset_model(num_files=1)]
    elif request.param == "activity":
        activity = activity_model()
        return [activity, activity.association.plan]
    elif request.param == "plan":
        return [plan_model()]
    else:
        return request.param


@pytest.mark.parametrize(
    "input_objects,expected_output",
    [
        ([], []),
        (
            "dataset",
            [
                {
                    "@id": "/dataset-files/2ac728d65fec48afbfa5e027eb3abd71",
                    "@type": ["http://schema.org/DigitalDocument", "http://www.w3.org/ns/prov#Entity"],
                    "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://www.w3.org/ns/prov#entity": [
                        {
                            "@id": (
                                "/entities/de3485bdebe74560ad386722842b06f6/data/my-dataset"
                                "/de3485bdebe74560ad386722842b06f6"
                            )
                        }
                    ],
                    "https://swissdatasciencecenter.github.io/renku-ontology#external": [{"@value": False}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#source": [{"@value": "None"}],
                },
                {
                    "@id": "/datasets/14249f1571fb4a2786ddeb7f706b9833",
                    "@type": ["http://schema.org/Dataset", "http://www.w3.org/ns/prov#Entity"],
                    "http://schema.org/creator": [],
                    "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/hasPart": [{"@id": "/dataset-files/2ac728d65fec48afbfa5e027eb3abd71"}],
                    "http://schema.org/identifier": [{"@value": "14249f1571fb4a2786ddeb7f706b9833"}],
                    "http://schema.org/image": [],
                    "http://schema.org/keywords": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#originalIdentifier": [
                        {"@value": "14249f1571fb4a2786ddeb7f706b9833"}
                    ],
                    "https://swissdatasciencecenter.github.io/renku-ontology#slug": [{"@value": "my-dataset"}],
                },
                {
                    "@id": (
                        "/entities/de3485bdebe74560ad386722842b06f6/data/my-dataset" "/de3485bdebe74560ad386722842b06f6"
                    ),
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#atLocation": [{"@value": "data/my-dataset/0"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#checksum": [{"@value": "0"}],
                },
                {
                    "@id": "/projects/my-project",
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasDataset": [
                        {"@id": "/datasets/14249f1571fb4a2786ddeb7f706b9833"}
                    ],
                },
            ],
        ),
        (
            [
                MagicMock(
                    spec=DatasetTag,
                    dataset_id=Url(url="/datasets/abcdefg1234567"),
                    date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
                    id="/dataset-tags/my-tag",
                    name="my-tag",
                )
            ],
            [
                {
                    "@id": "/dataset-tags/my-tag",
                    "@type": ["http://schema.org/PublicationEvent"],
                    "http://schema.org/about": [{"@id": "/urls/datasets/abcdefg1234567"}],
                    "http://schema.org/startDate": [{"@value": "2022-07-12T16:29:14+02:00"}],
                },
                {
                    "@id": "/urls/datasets/abcdefg1234567",
                    "@type": ["http://schema.org/URL"],
                    "http://schema.org/url": [{"@value": "/datasets/abcdefg1234567"}],
                },
            ],
        ),
        ([MagicMock(spec=OOBTree), MagicMock(spec=Index), MagicMock(spec=Catalog)], []),
        (
            "activity",
            [
                {
                    "@id": "/activities/c1ba59c308ab4c4d9edba71779cfe059",
                    "@type": ["http://www.w3.org/ns/prov#Activity"],
                    "http://www.w3.org/ns/prov#endedAtTime": [
                        {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-12T16:29:14+02:00"}
                    ],
                    "http://www.w3.org/ns/prov#qualifiedAssociation": [
                        {"@id": "/activities/c1ba59c308ab4c4d9edba71779cfe059/association"}
                    ],
                    "http://www.w3.org/ns/prov#qualifiedUsage": [],
                    "http://www.w3.org/ns/prov#startedAtTime": [
                        {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-12T16:29:14+02:00"}
                    ],
                    "http://www.w3.org/ns/prov#wasAssociatedWith": [{"@id": "mailto:john.doe@example.com"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#parameter": [],
                },
                {
                    "@id": "/activities/c1ba59c308ab4c4d9edba71779cfe059/association",
                    "@type": ["http://www.w3.org/ns/prov#Association"],
                    "http://www.w3.org/ns/prov#agent": [{"@id": "mailto:john.doe@example.com"}],
                    "http://www.w3.org/ns/prov#hadPlan": [{"@id": "/plans/7f8bcaa36ef844528b88230343503163"}],
                },
                {
                    "@id": "/plans/7f8bcaa36ef844528b88230343503163",
                    "@type": [
                        "http://schema.org/Action",
                        "http://schema.org/CreativeWork",
                        "http://www.w3.org/ns/prov#Plan",
                        "https://swissdatasciencecenter.github.io/renku-ontology#Plan",
                    ],
                    "http://schema.org/creator": [{"@id": "mailto:john.doe@example.com"}],
                    "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/keywords": [],
                    "http://schema.org/name": [{"@value": "my-plan"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#command": [{"@value": "echo"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasArguments": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasInputs": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasOutputs": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#successCodes": [],
                },
                {
                    "@id": "/projects/my-project",
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasActivity": [
                        {"@id": "/activities/c1ba59c308ab4c4d9edba71779cfe059"}
                    ],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasPlan": [
                        {"@id": "/plans/7f8bcaa36ef844528b88230343503163"}
                    ],
                },
                {
                    "@id": "mailto:john.doe@example.com",
                    "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
                    "http://schema.org/email": [{"@value": "john.doe@example.com"}],
                    "http://schema.org/name": [{"@value": "John Doe"}],
                },
            ],
        ),
        (
            "plan",
            [
                {
                    "@id": "/plans/7f8bcaa36ef844528b88230343503163",
                    "@type": [
                        "http://schema.org/Action",
                        "http://schema.org/CreativeWork",
                        "http://www.w3.org/ns/prov#Plan",
                        "https://swissdatasciencecenter.github.io/renku-ontology#Plan",
                    ],
                    "http://schema.org/creator": [{"@id": "mailto:john.doe@example.com"}],
                    "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
                    "http://schema.org/keywords": [],
                    "http://schema.org/name": [{"@value": "my-plan"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#command": [{"@value": "echo"}],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasArguments": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasInputs": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasOutputs": [],
                    "https://swissdatasciencecenter.github.io/renku-ontology#successCodes": [],
                },
                {
                    "@id": "/projects/my-project",
                    "https://swissdatasciencecenter.github.io/renku-ontology#hasPlan": [
                        {"@id": "/plans/7f8bcaa36ef844528b88230343503163"}
                    ],
                },
                {
                    "@id": "mailto:john.doe@example.com",
                    "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
                    "http://schema.org/email": [{"@value": "john.doe@example.com"}],
                    "http://schema.org/name": [{"@value": "John Doe"}],
                },
            ],
        ),
    ],
    indirect=["input_objects"],
)
def test_get_graph_for_revision(input_objects, expected_output):
    """Test getting a graph for a revision."""
    database_gateway = MagicMock(spec=IDatabaseGateway)
    database_gateway.get_modified_objects_from_revision.return_value = input_objects

    project_gateway = MagicMock(spec=IProjectGateway)
    project_gateway.get_project.return_value = MagicMock(spec=Project, id="/projects/my-project")

    result = get_graph_for_revision(
        revision_or_range="abcdefg", database_gateway=database_gateway, project_gateway=project_gateway
    )

    assert not DeepDiff(result, expected_output, ignore_order=True, exclude_regex_paths=r"root.*\['@id'\]")


def test_graph_export_full():
    """Test getting full graph."""

    dataset_gateway = MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [
        MagicMock(
            spec=Dataset,
            id="/datasets/abcdefg12345",
            slug="my-dataset",
            dataset_files=[
                DatasetFile(
                    id="/dataset-files/abcdefg123456789",
                    date_added=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
                    entity=Entity(path="data/my-dataset", checksum="1234567890"),
                )
            ],
            derived_from=Url(url_id="/datasets/0000000aaaaaaa"),
            initial_identifier="abcdefg",
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_removed=None,
            date_published=None,
        )
    ]
    dataset_gateway.get_by_id.return_value = Dataset(
        id="/datasets/0000000aaaaaaa",
        slug="my-dataset",
        date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        date_removed=None,
        date_published=None,
        identifier="abcdefg",
        initial_identifier="abcdefg",
    )
    dataset_gateway.get_all_tags.return_value = [
        MagicMock(
            spec=DatasetTag,
            dataset_id=Url(url="/datasets/abcdefg12345"),
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            id="/dataset-tags/my-tag",
            name="my-tag",
        )
    ]

    plan = Plan(
        id="/plans/abcdefg123456",
        command="echo",
        name="echo",
        date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        creators=[Person(email="test@example.com", name="John Doe")],
    )

    activity_gateway = MagicMock(spec=IActivityGateway)
    activity_gateway.get_all_activities.return_value = [
        MagicMock(
            spec=Activity,
            id="/activities/abcdefg123456",
            association=Association(
                id="/activities/abcdefg123456/association",
                agent=Person(email="test@example.com", name="John Doe"),
                plan=plan,
            ),
            started_at_time=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            ended_at_time=datetime.fromisoformat("2022-07-12T16:29:15+02:00"),
        )
    ]

    plan_gateway = MagicMock(spec=IPlanGateway)
    plan_gateway.get_all_plans.return_value = [
        CompositePlan(
            id="/plans/composite1",
            name="composite",
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            creators=[Person(email="test@example.com", name="John Doe")],
            plans=[plan],
        ),
        plan,
    ]

    project_gateway = MagicMock(spec=IProjectGateway)
    project_gateway.get_project.return_value = MagicMock(
        spec=Project,
        id="/projects/my-project",
        date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        image=None,
    )

    result = get_graph_for_all_objects(
        project_gateway=project_gateway,
        dataset_gateway=dataset_gateway,
        activity_gateway=activity_gateway,
        plan_gateway=plan_gateway,
    )

    expected_output = [
        {
            "@id": "/activities/abcdefg123456",
            "@type": ["http://www.w3.org/ns/prov#Activity"],
            "http://www.w3.org/ns/prov#qualifiedAssociation": [{"@id": "/activities/abcdefg123456/association"}],
            "http://www.w3.org/ns/prov#endedAtTime": [
                {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-12T16:29:15+02:00"}
            ],
            "http://www.w3.org/ns/prov#startedAtTime": [
                {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-12T16:29:14+02:00"}
            ],
        },
        {
            "@id": "/activities/abcdefg123456/association",
            "@type": ["http://www.w3.org/ns/prov#Association"],
            "http://www.w3.org/ns/prov#agent": [{"@id": "mailto:test@example.com"}],
            "http://www.w3.org/ns/prov#hadPlan": [{"@id": "/plans/abcdefg123456"}],
        },
        {
            "@id": "/plans/abcdefg123456",
            "@type": [
                "http://schema.org/Action",
                "http://schema.org/CreativeWork",
                "http://www.w3.org/ns/prov#Plan",
                "https://swissdatasciencecenter.github.io/renku-ontology#Plan",
            ],
            "http://schema.org/creator": [{"@id": "mailto:test@example.com"}],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/keywords": [],
            "http://schema.org/name": [{"@value": "echo"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#command": [{"@value": "echo"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasArguments": [],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasInputs": [],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasOutputs": [],
            "https://swissdatasciencecenter.github.io/renku-ontology#successCodes": [],
        },
        {
            "@id": "/plans/composite1",
            "@type": [
                "http://schema.org/Action",
                "http://schema.org/CreativeWork",
                "http://www.w3.org/ns/prov#Plan",
                "https://swissdatasciencecenter.github.io/renku-ontology#CompositePlan",
            ],
            "http://schema.org/creator": [{"@id": "mailto:test@example.com"}],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/keywords": [],
            "http://schema.org/name": [{"@value": "composite"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasMappings": [],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasSubprocess": [{"@id": "/plans/abcdefg123456"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#workflowLinks": [],
        },
        {
            "@id": "/projects/my-project",
            "https://swissdatasciencecenter.github.io/renku-ontology#hasPlan": [
                {"@id": "/plans/composite1"},
                {"@id": "/plans/abcdefg123456"},
            ],
        },
        {
            "@id": "/projects/my-project",
            "https://swissdatasciencecenter.github.io/renku-ontology#hasActivity": [
                {"@id": "/activities/abcdefg123456"}
            ],
            "https://swissdatasciencecenter.github.io/renku-ontology#hasPlan": [
                {"@id": "/plans/abcdefg123456"},
            ],
        },
        {
            "@id": "mailto:test@example.com",
            "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
            "http://schema.org/email": [{"@value": "test@example.com"}],
            "http://schema.org/name": [{"@value": "John Doe"}],
        },
        {
            "@id": "/projects/my-project",
            "@type": ["http://schema.org/Project", "http://www.w3.org/ns/prov#Location"],
            "http://schema.org/keywords": [],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
        },
        {
            "@id": "/dataset-files/abcdefg123456789",
            "@type": ["http://schema.org/DigitalDocument", "http://www.w3.org/ns/prov#Entity"],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://www.w3.org/ns/prov#entity": [{"@id": "/entities/1234567890/data/my-dataset"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#external": [{"@value": False}],
            "https://swissdatasciencecenter.github.io/renku-ontology#source": [{"@value": "None"}],
        },
        {
            "@id": "/datasets/abcdefg12345",
            "@type": ["http://schema.org/Dataset", "http://www.w3.org/ns/prov#Entity"],
            "http://schema.org/hasPart": [{"@id": "/dataset-files/abcdefg123456789"}],
            "http://www.w3.org/ns/prov#wasDerivedFrom": [{"@id": "/urls/datasets/0000000aaaaaaa"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#originalIdentifier": [{"@value": "abcdefg"}],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#slug": [{"@value": "my-dataset"}],
        },
        {
            "@id": "/entities/1234567890/data/my-dataset",
            "@type": ["http://www.w3.org/ns/prov#Entity"],
            "http://www.w3.org/ns/prov#atLocation": [{"@value": "data/my-dataset"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#checksum": [{"@value": "1234567890"}],
        },
        {
            "@id": "/projects/my-project",
            "https://swissdatasciencecenter.github.io/renku-ontology#hasDataset": [{"@id": "/datasets/abcdefg12345"}],
        },
        {
            "@id": "/urls/datasets/0000000aaaaaaa",
            "@type": ["http://schema.org/URL"],
            "http://schema.org/url": [{"@id": "/datasets/0000000aaaaaaa"}],
        },
        {
            "@id": "/dataset-tags/my-tag",
            "@type": ["http://schema.org/PublicationEvent"],
            "http://schema.org/about": [{"@id": "/urls/datasets/abcdefg12345"}],
            "http://schema.org/startDate": [{"@value": "2022-07-12T16:29:14+02:00"}],
        },
        {
            "@id": "/urls/datasets/abcdefg12345",
            "@type": ["http://schema.org/URL"],
            "http://schema.org/url": [{"@value": "/datasets/abcdefg12345"}],
        },
        {
            "@id": "/datasets/0000000aaaaaaa",
            "@type": ["http://schema.org/Dataset", "http://www.w3.org/ns/prov#Entity"],
            "http://schema.org/creator": [],
            "http://schema.org/dateCreated": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/dateModified": [{"@value": "2022-07-12T16:29:14+02:00"}],
            "http://schema.org/hasPart": [],
            "http://schema.org/identifier": [{"@value": "abcdefg"}],
            "http://schema.org/image": [],
            "http://schema.org/keywords": [],
            "https://swissdatasciencecenter.github.io/renku-ontology#originalIdentifier": [{"@value": "abcdefg"}],
            "https://swissdatasciencecenter.github.io/renku-ontology#slug": [{"@value": "my-dataset"}],
        },
        {
            "@id": "/projects/my-project",
            "https://swissdatasciencecenter.github.io/renku-ontology#hasDataset": [{"@id": "/datasets/0000000aaaaaaa"}],
        },
    ]
    assert not DeepDiff(result, expected_output, ignore_order=True, exclude_regex_paths=r"root.*\['@id'\]")


@pytest.mark.parametrize(
    "input_dict,expected_dict",
    [
        (
            {
                "@id": "mailto:test@example.com",
                "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
                "http://schema.org/email": [{"@value": "test@example.com"}],
                "http://schema.org/name": [{"@value": "John Doe"}],
            },
            {
                "@id": "mailto:test@example.com",
                "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
                "http://schema.org/email": [{"@value": "test@example.com"}],
                "http://schema.org/name": [{"@value": "John Doe"}],
            },
        ),
        ({"@id": "http://example.com/myid"}, {"@id": "http://example.com/myid"}),
        ({"@id": "https://example.com/myid"}, {"@id": "https://example.com/myid"}),
        (
            {
                "@id": "/urls/datasets/0000000aaaaaaa",
                "@type": ["http://schema.org/URL"],
                "http://schema.org/url": [{"@id": "/datasets/0000000aaaaaaa"}],
            },
            {
                "@id": "https://renkulab.io/urls/datasets/0000000aaaaaaa",
                "@type": ["http://schema.org/URL"],
                "http://schema.org/url": [{"@id": "https://renkulab.io/datasets/0000000aaaaaaa"}],
            },
        ),
    ],
)
def test_graph_replace_host(input_dict, expected_dict):
    """Test replacing host in graph export nodes."""

    update_nested_node_host(input_dict, "renkulab.io")

    assert input_dict == expected_dict


def test_graph_view_model():
    """Test graph view model methods."""

    model = GraphViewModel(
        [
            {
                "@id": "https://localhost/datasets/dbd380b2c18a4caeadecd01c7cb1257a",
                "@type": ["http://schema.org/Dataset", "http://www.w3.org/ns/prov#Entity"],
                "http://schema.org/creator": [{"@id": "mailto:john.doe@example.com"}],
                "http://schema.org/dateCreated": [{"@value": "2022-06-02T15:36:49+02:00"}],
                "http://schema.org/hasPart": [],
                "http://schema.org/identifier": [{"@value": "dbd380b2c18a4caeadecd01c7cb1257a"}],
                "http://schema.org/image": [],
                "http://schema.org/keywords": [],
                "http://schema.org/name": [{"@value": "e"}],
                "https://swissdatasciencecenter.github.io/renku-ontology#originalIdentifier": [
                    {"@value": "dbd380b2c18a4caeadecd01c7cb1257a"}
                ],
                "https://swissdatasciencecenter.github.io/renku-ontology#slug": [{"@value": "e"}],
            },
            {
                "@id": "https://localhost/projects/john.doe/test-dataset",
                "https://swissdatasciencecenter.github.io/renku-ontology#hasDataset": [
                    {"@id": "https://localhost/datasets/dbd380b2c18a4caeadecd01c7cb1257a"}
                ],
            },
            {
                "@id": "mailto:john.doe@example.com",
                "@type": ["http://schema.org/Person", "http://www.w3.org/ns/prov#Person"],
                "http://schema.org/email": [{"@value": "john.doe@example.com"}],
                "http://schema.org/name": [{"@value": "John Doe"}],
            },
        ]
    )

    result = model.as_jsonld()
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert len(result) == 3

    result = model.as_jsonld_string()
    assert isinstance(result, str)
    len_indentation_of_2 = len(result)
    assert len_indentation_of_2 > 1500

    result = model.as_jsonld_string(indentation=None)
    assert isinstance(result, str)
    len_no_indentation = len(result)
    assert len_no_indentation > 1100
    assert len_no_indentation != len_indentation_of_2

    result = model.as_nt_string()
    assert isinstance(result, str)
    assert len(result) > 1600

    result = model.as_rdf_string()
    assert isinstance(result, str)
    assert len(result) > 1200

    result = model.as_dot_string()
    assert isinstance(result, str)
    assert len(result) > 2600

    result = model.as_rdflib_graph()
    assert isinstance(result, Graph)
    assert len(result.all_nodes()) == 12
