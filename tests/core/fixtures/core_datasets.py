# -*- coding: utf-8 -*-
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
"""Renku core fixtures for datasets testing."""

from contextlib import contextmanager

import pytest


@pytest.fixture
def dataset_responses():
    """Authentication responses."""
    import responses

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def request_callback(request):
            return 200, {"Content-Type": "application/text"}, "123"

        response.add_callback(responses.GET, "http://example.com/file1", callback=request_callback)
        response.add_callback(responses.GET, "https://example.com/file1", callback=request_callback)
        response.add_callback(responses.GET, "http://example.com/file1.ext?foo=bar", callback=request_callback)
        response.add_callback(responses.HEAD, "http://example.com/file1", callback=request_callback)
        response.add_callback(responses.HEAD, "https://example.com/file1", callback=request_callback)
        response.add_callback(responses.HEAD, "http://example.com/file1.ext?foo=bar", callback=request_callback)

        yield response


@pytest.fixture
def client_with_injection(client, client_database_injection_manager):
    """Return a Renku repository with injected dependencies."""

    with client_database_injection_manager(client):
        yield client


@pytest.fixture
def client_with_datasets(client, directory_tree, client_database_injection_manager):
    """A client with datasets."""
    from renku.core.models.provenance.agent import Person

    person_1 = Person.from_string("P1 <p1@example.com> [IANA]")
    person_2 = Person.from_string("P2 <p2@example.com>")

    with client_database_injection_manager(client):
        client.create_dataset(name="dataset-1", keywords=["dataset", "1"], creators=[person_1])

        with client.with_dataset(name="dataset-2", create=True, commit_database=True) as dataset:
            dataset.keywords = ["dataset", "2"]
            dataset.creators = [person_1, person_2]

            client.add_data_to_dataset(dataset=dataset, urls=[str(p) for p in directory_tree.glob("*")])

    client.repo.git.add("--all")
    client.repo.index.commit("add files to datasets")

    yield client


@pytest.fixture
def load_dataset_with_injection(client_database_injection_manager):
    """Load dataset method with injection setup."""

    def _inner(name, client):
        from tests.utils import load_dataset

        with client_database_injection_manager(client):
            return load_dataset(name)

    return _inner


@pytest.fixture
def get_datasets_provenance_with_injection(client_database_injection_manager):
    """Get dataset provenance method with injection setup."""

    @contextmanager
    def _inner(client):
        from renku.core.management.datasets import DatasetsProvenance

        with client_database_injection_manager(client):
            yield DatasetsProvenance()

    return _inner
