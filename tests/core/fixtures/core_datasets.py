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

from typing import Generator

import pytest

from renku.core.dataset.dataset import create_dataset
from renku.core.dataset.dataset_add import add_to_dataset
from tests.fixtures.repository import RenkuProject


@pytest.fixture
def dataset_responses():
    """Authentication responses."""
    import responses

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def request_callback(request):
            return 200, {"Content-Type": "application/text"}, "file1 content"

        response.add_callback(responses.GET, "http://example.com/file1", callback=request_callback)
        response.add_callback(responses.GET, "https://example.com/file1", callback=request_callback)
        response.add_callback(responses.GET, "http://example.com/file1.ext?foo=bar", callback=request_callback)
        response.add_callback(responses.HEAD, "http://example.com/file1", callback=request_callback)
        response.add_callback(responses.HEAD, "https://example.com/file1", callback=request_callback)
        response.add_callback(responses.HEAD, "http://example.com/file1.ext?foo=bar", callback=request_callback)

        yield response


@pytest.fixture
def project_with_datasets(project, directory_tree, with_injection) -> Generator[RenkuProject, None, None]:
    """A project with datasets."""
    from renku.domain_model.provenance.agent import Person

    person_1 = Person.from_string("P1 <p1@example.com> [IANA]")
    person_2 = Person.from_string("P2 <p2@example.com>")

    with with_injection():
        create_dataset(slug="dataset-1", keywords=["dataset", "1"], creators=[person_1])

        dataset = add_to_dataset("dataset-2", urls=[str(p) for p in directory_tree.glob("*")], create=True, copy=True)
        dataset.keywords = ["dataset", "2"]
        dataset.creators = [person_1, person_2]

    project.repository.add(all=True)
    project.repository.commit("add files to datasets")

    yield project
