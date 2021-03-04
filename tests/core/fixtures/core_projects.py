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
"""Renku core fixtures for project management."""
import os
import tempfile

import pytest


@pytest.fixture
def local_client():
    """Add a Renku local client."""
    from renku.core.management import LocalClient

    with tempfile.TemporaryDirectory() as tempdir:
        yield LocalClient(path=tempdir)


@pytest.fixture
def reset_environment(svc_client, mock_redis):
    """Restore environment variable to their values before test execution."""
    current_environment = os.environ.copy()

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(current_environment)
