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
"""Renku CLI fixtures for Gateway."""
import json

import pytest
import responses
from _pytest.monkeypatch import MonkeyPatch

ENDPOINT = "renku.deployment.ch"
ACCESS_TOKEN = "jwt-token"
USER_CODE = "valid_user_code"


@pytest.fixture(scope="module")
def mock_login():
    """Monkey patch webbrowser module for renku login."""
    import webbrowser

    with MonkeyPatch().context() as monkey_patch:
        monkey_patch.setattr(webbrowser, "open_new_tab", lambda _: None)

        with responses.RequestsMock(assert_all_requests_are_fired=False) as requests_mock:

            def callback(token):
                def func(request):
                    if request.params.get("server_nonce") == USER_CODE:
                        return 200, {"Content-Type": "application/json"}, json.dumps({"access_token": token})

                    return 404, {"Content-Type": "application/json"}, ""

                return func

            requests_mock.add_passthru("https://pypi.org/")

            class RequestMockWrapper:
                @staticmethod
                def add_endpoint_token(endpoint, token):
                    """Add a mocked endpoint and its access token."""
                    requests_mock.add_callback(
                        responses.GET, f"https://{endpoint}/api/auth/cli-token", callback=callback(token)
                    )

            RequestMockWrapper.add_endpoint_token(ENDPOINT, ACCESS_TOKEN)

            yield RequestMockWrapper
