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
"""Renku service templates read manifest controller tests."""
import pytest
from git import GitCommandError
from marshmallow import ValidationError


def test_template_read_manifest_ctrl(ctrl_init, svc_client_with_templates, mocker):
    """Test template read manifest controller."""
    from renku.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl

    cache, user_data = ctrl_init
    svc_client, headers, template_params = svc_client_with_templates

    ctrl = TemplatesReadManifestCtrl(cache, user_data, template_params)
    ctrl_mock = mocker.patch.object(
        ctrl, "template_manifest", return_value=[{"name": "test", "folder": ".", "description": "desc"}]
    )

    response = ctrl.to_response()
    assert {"result": {"templates": ctrl_mock.return_value}} == response.json


@pytest.mark.parametrize("git_url", ["https://github.com", "https://github.com/SwissDataScienceCenter",])
def test_validation_exc_template_read_manifest_ctrl(git_url, ctrl_init, svc_client_with_templates, mocker):
    """Test validation exception on template read manifest controller."""
    from renku.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl

    cache, user_data = ctrl_init
    svc_client, headers, template_params = svc_client_with_templates
    template_params["url"] = git_url

    with pytest.raises(ValidationError):
        TemplatesReadManifestCtrl(cache, user_data, template_params)


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.parametrize("git_url", ["https://test.com/test2/test3", "https://www.test.com/test2/test3"])
def test_found_exc_template_read_manifest_ctrl(git_url, ctrl_init, svc_client_with_templates, mocker):
    """Test git command exception on template read manifest controller."""
    from renku.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl

    cache, user_data = ctrl_init
    svc_client, headers, template_params = svc_client_with_templates
    template_params["url"] = git_url

    ctrl = TemplatesReadManifestCtrl(cache, user_data, template_params)

    with pytest.raises(GitCommandError):
        ctrl.to_response()
