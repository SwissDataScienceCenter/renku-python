#
# Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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

import re

import pytest

from renku.core.lfs import get_lfs_migrate_filters, track_paths_in_storage
from renku.domain_model.project_context import project_context


@pytest.mark.parametrize("path", [".", "datasets"])
def test_no_renku_metadata_in_lfs(project_with_datasets, no_lfs_size_limit, path, subdirectory, with_injection):
    """Test .renku directory and its content are not included in the LFS."""
    # Explicitly set .renku to not being ignored
    (project_with_datasets.path / ".renkulfsignore").write_text("!.renku")

    file1 = project_with_datasets.path / "file1"
    file1.write_text("123")
    path_in_renku_metadata_directory = project_context.database_path.parent / path
    path_in_renku_metadata_directory.mkdir(parents=True, exist_ok=True)
    file2 = path_in_renku_metadata_directory / "file2"
    file2.write_text("123")

    with with_injection():
        track_paths_in_storage(file1, file2, path_in_renku_metadata_directory)

    attributes = (project_with_datasets.path / ".gitattributes").read_text()
    assert "file1" in attributes
    assert "file2" not in attributes
    assert not re.match("^renku/.* filter=lfs diff=lfs merge=lfs -text$", attributes)


def test_renku_in_lfs_migrate_exclude_filter(project):
    """Test .renku directory is in exclude filter of `lfs migrate info`."""
    _, excludes = get_lfs_migrate_filters()

    assert ",.renku," in excludes[1]
    assert ",.renku/**," in excludes[1]
