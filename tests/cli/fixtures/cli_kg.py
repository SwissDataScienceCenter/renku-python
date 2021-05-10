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
"""Renku CLI fixtures for KG integration."""
import json
import re

import pytest


@pytest.fixture
def missing_kg_project_responses():
    """KG project query responses for missing project."""
    import responses

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def request_callback(request):
            return 404, {"Content-Type": "application/text"}, json.dumps({"message": "no project found"})

        response.add_callback(
            responses.GET, re.compile("http(s)*://dev.renku.ch/knowledge-graph/projects/.*"), callback=request_callback
        )

        response.add_passthru(re.compile("http(s)*://dev.renku.ch/datasets/.*"))
        response.add_passthru(re.compile("http(s)*://dev.renku.ch/knowledge-graph/datasets/.*"))

        yield response


@pytest.fixture
def mock_kg():
    """Mock KG responses for dataset import."""
    import responses

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def callback(request):
            authorization = request.headers.get("Authorization")
            status_code = 404 if authorization == "Bearer renku-token" else 401
            return status_code, {"Content-Type": "application/json"}, ""

        response.add_passthru("https://pypi.org/")
        response.add_callback(responses.GET, re.compile("http(s)*://renku.ch/api/kg/.*"), callback=callback)
        response.add_callback(responses.GET, re.compile("http(s)*://renku.ch/knowledge-graph/.*"), callback=callback)

        yield response
