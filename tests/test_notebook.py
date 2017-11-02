# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Test Python SDK client."""

import pytest

from renga.notebook import RengaStorageManager


def test_file_manager(instance_path, renga_client, monkeypatch,
                      deployer_responses, storage_responses):
    """Test file manager."""
    client = renga_client

    monkeypatch.setenv('RENGA_ENDPOINT', client.api.endpoint)
    monkeypatch.setenv('RENGA_ACCESS_TOKEN', client.api.token['access_token'])
    monkeypatch.setenv('RENGA_CONTEXT_ID', 'abcd')

    contents_manager = RengaStorageManager()
    contents_manager._save_notebook('current_context/inputs/notebook', {})
