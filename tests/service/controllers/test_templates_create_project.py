# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service templates create project controller tests."""
from renku.core.utils.scm import strip_and_lower


def test_template_create_project_ctrl(ctrl_init, svc_client_templates_creation, mocker):
    """Test template create project controller."""
    from renku.service.controllers.templates_create_project import TemplatesCreateProjectCtrl

    cache, user_data = ctrl_init
    svc_client, headers, payload, rm_remote = svc_client_templates_creation

    ctrl = TemplatesCreateProjectCtrl(cache, user_data, payload)
    ctrl_mock = mocker.patch.object(ctrl, "new_project_push", return_value=None)
    response = ctrl.to_response()

    # Check response.
    assert {"result"} == response.json.keys()
    assert {"url", "namespace", "name"} == response.json["result"].keys()

    # Check ctrl_mock.
    assert ctrl_mock.call_count == 1
    assert response.json["result"]["name"] == ctrl_mock.call_args[0][0].name

    # Ctrl state.
    expected_context = {
        "timestamp",
        "owner",
        "project_namespace",
        "token",
        "email",
        "project_repository",
        "project_id",
        "url",
        "identifier",
        "initialized",
        "parameters",
        "project_name",
        "name",
        "new_project_url",
        "fullname",
        "project_slug",
        "git_url",
        "project_name_stripped",
        "depth",
        "ref",
        "new_project_url_with_auth",
        "url_with_auth",
        "user_id",
    }
    assert expected_context == set(ctrl.context.keys())

    received_metadata = ctrl.default_metadata
    expected_metadata = {
        "__template_source__",
        "__template_ref__",
        "__template_id__",
        "__namespace__",
        "__automated_update__",
        "__repository__",
        "__sanitized_project_name__",
        "__project_slug__",
    }
    assert expected_metadata == set(received_metadata.keys())

    assert payload["url"] == received_metadata["__template_source__"]
    assert payload["ref"] == received_metadata["__template_ref__"]
    assert payload["identifier"] == received_metadata["__template_id__"]
    assert payload["project_namespace"] == received_metadata["__namespace__"]
    assert payload["project_repository"] == received_metadata["__repository__"]

    project_name = strip_and_lower(payload["project_name"])
    assert project_name == received_metadata["__sanitized_project_name__"]
    assert f"{payload['project_namespace']}/{project_name}" == received_metadata["__project_slug__"]
