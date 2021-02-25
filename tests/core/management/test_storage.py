# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Storage tests."""

import pytest


@pytest.mark.parametrize("path", [".", "datasets"])
def test_no_renku_metadata_in_lfs(client_with_datasets, no_lfs_size_limit, path, subdirectory):
    """Test .renku directory and its content are not included in the LFS."""
    # Explicitly set .renku to not being ignored
    (client_with_datasets.path / ".renkulfsignore").write_text("!.renku")

    file1 = client_with_datasets.path / "file1"
    file1.write_text("123")
    path_in_renku_metadata_directory = client_with_datasets.renku_metadata_path.parent / path
    file2 = path_in_renku_metadata_directory / "file2"
    file2.write_text("123")

    client_with_datasets.track_paths_in_storage(file1, file2, path_in_renku_metadata_directory)

    attributes = (client_with_datasets.path / ".gitattributes").read_text()
    assert "file1" in attributes
    assert "file2" not in attributes
    assert ".renku" not in attributes


def test_renku_in_lfs_migrate_exclude_filter(client):
    """Test .renku directory is in exclude filter of `lfs migrate info`."""
    _, excludes, _ = client.get_lfs_migrate_filters()

    assert excludes[1].endswith(".renku")
