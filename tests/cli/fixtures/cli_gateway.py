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
"""Renku CLI fixtures for Gateway."""

import json
import urllib

import pytest
import responses
from _pytest.monkeypatch import MonkeyPatch

ENDPOINT = "renku.deployment.ch"
ACCESS_TOKEN = "jwt-token"
DEVICE_CODE = "valid-device-code"
IMAGE_REGISTRY_HOST = "registry.renku.deployment.ch"


@pytest.fixture(scope="module")
def mock_login():
    """Monkey patch webbrowser package and keycloak endpoints for renku login."""
    import webbrowser

    with MonkeyPatch().context() as monkey_patch:
        monkey_patch.setattr(webbrowser, "open_new_tab", lambda _: True)

        with responses.RequestsMock(assert_all_requests_are_fired=False) as requests_mock:

            def device_callback(request):
                data = dict(urllib.parse.parse_qsl(request.body))
                if data.get("client_id") != "renku-cli":
                    return 400, {"Content-Type": "application/json"}, json.dumps({"error": "invalid_client"})

                data = {
                    "verification_uri": f"https://{ENDPOINT}/auth/realms/Renku/device",
                    "user_code": "ABC-DEF",
                    "interval": 0,
                    "device_code": DEVICE_CODE,
                }
                return 200, {"Content-Type": "application/json"}, json.dumps(data)

            def create_token_callback(token):
                def token_callback(request):
                    data = dict(urllib.parse.parse_qsl(request.body))
                    if (
                        data.get("device_code") == DEVICE_CODE
                        and data.get("client_id") == "renku-cli"
                        and data.get("grant_type") == "urn:ietf:params:oauth:grant-type:device_code"
                    ):
                        return 200, {"Content-Type": "application/json"}, json.dumps({"access_token": token})

                    return 400, {"Content-Type": "application/json"}, ""

                return token_callback

            def create_image_registry_host_callback(host):
                def image_registry_host_callback(_):
                    return 200, {"Content-Type": "application/json"}, json.dumps({"default": host})

                return image_registry_host_callback

            requests_mock.add_passthru("https://pypi.org/")

            class RequestMockWrapper:
                @staticmethod
                def add_device_auth(endpoint, token):
                    """Add a mocked endpoint."""
                    requests_mock.add_callback(
                        responses.POST,
                        f"https://{endpoint}/auth/realms/Renku/protocol/openid-connect/auth/device",
                        callback=device_callback,
                    )
                    requests_mock.add_callback(
                        responses.POST,
                        f"https://{endpoint}/auth/realms/Renku/protocol/openid-connect/token",
                        callback=create_token_callback(token),
                    )

                @staticmethod
                def add_registry_image_host(endpoint, host):
                    requests_mock.add_callback(
                        responses.GET,
                        f"https://{endpoint}/api/config/imageRegistries",
                        callback=create_image_registry_host_callback(host),
                    )

            RequestMockWrapper.add_device_auth(ENDPOINT, ACCESS_TOKEN)
            RequestMockWrapper.add_registry_image_host(ENDPOINT, IMAGE_REGISTRY_HOST)

            yield RequestMockWrapper
