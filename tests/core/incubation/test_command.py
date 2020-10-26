# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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
"""Test command builder."""
from pathlib import Path

from renku.core.incubation.command import Command


def test_dataset_add_command(project, tmpdir):
    """test dataset add with builder."""
    from renku.core.commands.dataset import DATASET_METADATA_PATHS, _add_to_dataset, create_dataset, list_files

    add_to_dataset = (
        Command()
        .command(_add_to_dataset)
        .require_clean()
        .require_migration()
        .with_commit()
        .commit_only(DATASET_METADATA_PATHS)
        .raise_if_empty()
        .lock_project()
        .build()
    )

    create_dataset("ds1", title="", description="", creators=[], commit_message="my awesome dataset")
    data_file = tmpdir / Path("somefile")
    data_file.write_text("1,2,3", encoding="utf-8")

    add_to_dataset.execute([str(data_file)], "ds1")
    files = list_files(datasets=["ds1"])

    assert isinstance(files, list)
    assert "somefile" in [file_.name for file_ in files]
