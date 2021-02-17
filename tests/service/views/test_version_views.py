# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 -Swiss Data Science Center (SDSC)
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
"""Renku service version view tests."""
from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION


def test_version(svc_client):
    """Test expected response from version endpoint."""
    from renku import __version__

    response = svc_client.get("/version")
    assert "result" in response.json
    data = response.json["result"]

    assert {"latest_version", "supported_project_version"} == set(data.keys())
    assert __version__ == data["latest_version"]
    assert SUPPORTED_PROJECT_VERSION == data["supported_project_version"]
