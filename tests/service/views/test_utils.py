#
# Copyright 2019-2023 - Swiss Data Science Center (SDSC)
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
"""Renku service rpc serializers."""
from renku.ui.service.errors import ProgramRenkuError
from renku.ui.service.serializers.datasets import DatasetListResponseRPC
from renku.ui.service.views import error_response, result_response


def test_error_response(svc_client):
    """Test error response utility."""
    err_reason = "test error"
    error = Exception(err_reason)
    test_error = ProgramRenkuError(error)

    response = error_response(test_error).json

    assert response
    assert {"error"} == set(response.keys())
    assert {"code", "userMessage", "devMessage", "devReference"} == set(response["error"].keys())
    assert err_reason in response["error"]["devMessage"]


def test_result_response(svc_client):
    """Test result response utility."""
    ctx = {"datasets": [{"slug": "my-dataset"}]}
    response = result_response(DatasetListResponseRPC(), ctx).json

    assert response
    assert {"result"} == set(response.keys())
    assert {"datasets"} == set(response["result"].keys())

    expected = ctx["datasets"][0]
    received = response["result"]["datasets"][0]

    assert expected["slug"] == received["slug"]


def test_result_response_with_none(svc_client):
    """Test result response with None value."""
    response = result_response(DatasetListResponseRPC(), None).json

    assert response
    assert {"result"} == set(response.keys())
    assert response["result"] is None


def test_result_response_with_empty_dict(svc_client):
    """Test result response with empty value."""
    response = result_response(DatasetListResponseRPC(), {}).json

    assert response
    assert {"result"} == set(response.keys())
    assert {} == response["result"]


def test_result_response_with_empty_tuple(svc_client):
    """Test result response with empty value."""
    response = result_response(DatasetListResponseRPC(), ()).json

    assert response
    assert {"result"} == set(response.keys())
    assert {} == response["result"]
