# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Tests for Project API."""

import os
import pytest

from renku.api import Project


@pytest.mark.parametrize("sub_path", [".", "src", "src/notebooks"])
def test_get_project(client, sub_path):
    """Test getting Project context within a repository."""
    working_dir = client.path / sub_path
    working_dir.mkdir(exist_ok=True, parents=True)
    os.chdir(working_dir)

    with Project() as p:
        assert client.path == p.client.path
