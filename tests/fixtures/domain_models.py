#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku domain model fixtures."""

from datetime import datetime
from uuid import uuid4

import pytest


@pytest.fixture()
def empty_dataset_model():
    """Dataset without files."""
    from renku.domain_model.dataset import Dataset

    def _create_dataset(slug="my-dataset", identifier="5e77e63037614ea89309e21befe91dbb"):
        return Dataset(
            id=Dataset.generate_id(identifier),
            identifier=identifier,
            initial_identifier=identifier,
            slug=slug,
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        )

    yield _create_dataset


@pytest.fixture()
def dataset_model():
    """Dataset with files."""
    from renku.domain_model.dataset import Dataset, DatasetFile
    from renku.domain_model.entity import Entity

    def _create_dataset(slug="my-dataset", num_files=2, identifier="14249f1571fb4a2786ddeb7f706b9833"):
        files = []
        for i in range(num_files):
            path = f"data/{slug}/{i}"
            files.append(
                DatasetFile(
                    id=DatasetFile.generate_id(),
                    entity=Entity(id=Entity.generate_id(checksum=str(i), path=path), checksum=str(i), path=path),
                    date_added=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
                )
            )
        return Dataset(
            id=Dataset.generate_id(identifier),
            identifier=identifier,
            initial_identifier=identifier,
            slug=slug,
            dataset_files=files,
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        )

    yield _create_dataset


@pytest.fixture()
def derived_dataset_model(empty_dataset_model, dataset_model):
    """Dataset with modification."""

    def _create_dataset(slug="my-dataset", identifier="5d8e5f72ef21441291cbf19db5a417ce"):
        source_dataset = empty_dataset_model(slug)
        new_dataset = dataset_model(slug)
        new_dataset.derive_from(source_dataset, identifier=identifier)
        return new_dataset

    yield _create_dataset


@pytest.fixture()
def project_model():
    """Project object."""
    from renku.domain_model.project import Project
    from renku.domain_model.provenance.agent import Person

    def _create_project(namespace="john.doe", name="my-project", email="john.doe@example.com"):
        return Project(
            id=Project.generate_id(namespace, name),
            creator=Person(id=Person.generate_id(email=email, full_identity=namespace), email=email, name=name),
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        )

    yield _create_project


@pytest.fixture()
def plan_model():
    """Plan object."""
    from renku.domain_model.provenance.agent import Person
    from renku.domain_model.workflow.plan import Plan

    def _create_plan(name="my-plan", command="echo", identifier="7f8bcaa36ef844528b88230343503163"):
        return Plan(
            id=Plan.generate_id(uuid=identifier),
            name=name,
            command=command,
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            creators=[
                Person(
                    id=Person.generate_id(email="john.doe@example.com", full_identity="john.doe"),
                    email="john.doe@example.com",
                    name="John Doe",
                )
            ],
        )

    yield _create_plan


@pytest.fixture()
def composite_plan_model(plan_model):
    """`CompositePlan` object with steps."""
    from renku.domain_model.workflow.composite_plan import CompositePlan

    def _create_composite(name="my-composite", num_steps=2):
        steps = []

        for i in range(num_steps):
            steps.append(plan_model(name=f"my-plan-{i}", identifier=uuid4().hex))
        return CompositePlan(
            id=CompositePlan.generate_id(uuid="2ecdb9651ea745a4a419272f2451457c"),
            name=name,
            plans=steps,
            date_created=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            date_modified=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
        )

    yield _create_composite


@pytest.fixture()
def activity_model(plan_model):
    """Activity object."""
    from renku.domain_model.provenance.activity import Activity, Association
    from renku.domain_model.provenance.agent import Person

    def _create_activity():
        activity_id = Activity.generate_id("c1ba59c308ab4c4d9edba71779cfe059")
        agent = Person(email="john.doe@example.com", name="John Doe")
        return Activity(
            id=activity_id,
            agents=[agent],
            started_at_time=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            ended_at_time=datetime.fromisoformat("2022-07-12T16:29:14+02:00"),
            association=Association(id=Association.generate_id(activity_id), plan=plan_model(), agent=agent),
        )

    yield _create_activity
