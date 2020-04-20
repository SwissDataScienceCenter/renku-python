# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
from renku.service.serializers.datasets import DatasetListResponseRPC
from renku.service.views import error_response, result_response


def test_error_response(svc_client):
    """Test error response utility."""
    err_code = 0
    err_reason = 'test error'
    response = error_response(err_code, err_reason).json

    assert response
    assert {'error'} == set(response.keys())
    assert {'code', 'reason'} == set(response['error'].keys())

    assert err_code == response['error']['code']
    assert err_reason == response['error']['reason']


def test_result_response(svc_client):
    """Test result response utility."""
    ctx = {'datasets': [{'short_name': 'my-dataset'}]}
    response = result_response(DatasetListResponseRPC(), ctx).json

    assert response
    assert {'result'} == set(response.keys())
    assert {'datasets'} == set(response['result'].keys())

    expected = ctx['datasets'][0]
    received = response['result']['datasets'][0]

    assert expected['short_name'] == received['short_name']


def test_result_response_with_none(svc_client):
    """Test result response with None value."""
    response = result_response(DatasetListResponseRPC(), None).json

    assert response
    assert {'result'} == set(response.keys())
    assert response['result'] is None


def test_result_response_with_empty_dict(svc_client):
    """Test result response with empty value."""
    response = result_response(DatasetListResponseRPC(), {}).json

    assert response
    assert {'result'} == set(response.keys())
    assert {} == response['result']


def test_result_response_with_empty_tuple(svc_client):
    """Test result response with empty value."""
    response = result_response(DatasetListResponseRPC(), ()).json

    assert response
    assert {'result'} == set(response.keys())
    assert {} == response['result']
