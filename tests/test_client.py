# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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


def test_local_client(tmpdir):
    """Test a local client."""
    from renku.api.client import LocalClient
    client = LocalClient(str(tmpdir.mkdir('project')))

    assert client.path
    assert client.repo is None


@pytest.mark.parametrize(
    'paths, ignored', (
        (['.renku.lock'], ['.renku.lock']),
        (['not ignored', 'lib/foo', 'build/html'], ['lib/foo', 'build/html']),
        (['not ignored'], None),
    )
)
def test_ignored_paths(paths, ignored, client):
    """Test resolution of ignored paths."""
    assert client.find_ignored_paths(*paths) == ignored
