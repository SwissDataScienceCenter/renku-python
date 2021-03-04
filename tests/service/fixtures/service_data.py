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
"""Renku service fixtures for data management."""
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def datapack_zip(directory_tree):
    """Returns dummy data folder as a zip archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "zip", str(directory_tree))

    yield Path(workspace_dir.name) / "datapack.zip"


@pytest.fixture
def datapack_tar(directory_tree):
    """Returns dummy data folder as a tar archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "tar", str(directory_tree))

    yield Path(workspace_dir.name) / "datapack.tar"


@pytest.fixture
def datapack_gz(directory_tree):
    """Returns dummy data folder as a tar archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "gztar", str(directory_tree))

    shutil.move(Path(workspace_dir.name) / "datapack.tar.gz", Path(workspace_dir.name) / "datapack.gz")

    yield Path(workspace_dir.name) / "datapack.gz"
