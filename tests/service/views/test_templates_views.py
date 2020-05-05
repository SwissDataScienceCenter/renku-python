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
"""Renku service templates view tests."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from flaky import flaky
from tests.core.commands.test_init import TEMPLATE_ID, TEMPLATE_INDEX, \
    TEMPLATE_REF, TEMPLATE_URL

from renku.core.commands.init import fetch_template, read_template_manifest


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_read_manifest_from_template(svc_client_with_templates):
    """Check reading manifest template."""
    svc_client, headers, template_params = svc_client_with_templates

    response = svc_client.get(
        '/templates.read_manifest',
        query_string=template_params,  # data=json.dumps(payload)
        headers=headers
    )

    assert response
    assert {'result'} == set(response.json.keys())
    assert response.json['result']['templates']
    templates = response.json['result']['templates']
    assert len(templates) > 0
    default_template = templates[TEMPLATE_INDEX - 1]
    assert default_template['folder'] == TEMPLATE_ID


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_compare_manifests(svc_client_with_templates):
    """Check reading manifest template."""
    svc_client, headers, template_params = svc_client_with_templates

    response = svc_client.get(
        '/templates.read_manifest',
        query_string=template_params,
        headers=headers
    )

    assert response
    assert {'result'} == set(response.json.keys())
    assert response.json['result']['templates']

    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        manifest_file = fetch_template(TEMPLATE_URL, TEMPLATE_REF, temp_path)
        manifest = read_template_manifest(temp_path)

        assert manifest_file and manifest_file.exists()
        assert manifest

        templates_service = response.json['result']['templates']
        templates_local = manifest
        default_index = TEMPLATE_INDEX - 1
        assert templates_service[default_index
                                 ] == templates_local[default_index]
