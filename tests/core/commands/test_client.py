# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

import inspect
from pathlib import Path

import pytest

from renku.core.project.project_properties import project_properties


def test_local_client(tmpdir):
    """Test a local client."""
    with project_properties.with_path(Path(tmpdir.mkdir("project"))):
        assert project_properties.path

        with pytest.raises(ValueError):
            _ = project_properties.repository


@pytest.mark.parametrize(
    "paths, ignored",
    (
        ([".renku.lock"], [".renku.lock"]),
        (["not ignored", "lib/foo", "build/html"], ["lib/foo", "build/html"]),
        (["not ignored"], []),
    ),
)
def test_ignored_paths(paths, ignored, client):
    """Test resolution of ignored paths."""
    assert project_properties.repository.get_ignored_paths(*paths) == ignored


def test_safe_class_attributes(tmpdir):
    """Test that there are no unsafe class attributes on the client.

    This prevents us from adding class attributes that might leak in a threaded environment.
    If you do add a class attribute and want to add it to the list of safe_attributes,
    make sure that it's not something that can leak between calls, e.g. in the service.
    """
    from renku.core.management.client import LocalClient

    # NOTE: attributes that are allowed on LocalClient
    safe_attributes = []

    with project_properties.with_path(Path(tmpdir.mkdir("project1"))):
        client1 = LocalClient()

    with project_properties.with_path(Path(tmpdir.mkdir("project2"))):
        client2 = LocalClient()

    class_attributes = inspect.getmembers(LocalClient, lambda a: not (inspect.isroutine(a)))
    class_attributes = [a for a in class_attributes if not a[0].startswith("__") and not a[0].endswith("__")]
    identical_attributes = []
    for a, v in class_attributes:
        if a in safe_attributes or isinstance(v, property):
            continue
        if id(getattr(client1, a)) == id(getattr(client2, a)):
            identical_attributes.append(a)

    assert not identical_attributes
