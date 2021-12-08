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
"""Serializers for dataset list files."""

import re
from subprocess import PIPE, SubprocessError, run

from renku.core.commands.format.tabulate import tabulate
from renku.core.commands.schema.dataset import DatasetFileSchema
from renku.core.management.command_builder import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher


def tabular(records, *, columns=None):
    """Format dataset files with a tabular output.

    :param records: Filtered collection.
    :param columns: List of columns to display
    """

    if not columns:
        columns = "added,creators,dataset,full_path"

    if "size" in columns.split(","):
        _get_lfs_file_sizes(records)

    if "lfs" in columns.split(","):
        _get_lfs_tracking(records)

    for record in records:
        record.creators = record.dataset.creators

    return tabulate(
        collection=records,
        columns=columns,
        columns_mapping=DATASET_FILES_COLUMNS,
        columns_alignments=DATASET_FILES_COLUMNS_ALIGNMENTS,
    )


@inject.autoparams()
def _get_lfs_tracking(records, client_dispatcher: IClientDispatcher):
    """Check if files are tracked in git lfs."""
    client = client_dispatcher.current_client

    paths = (r.path for r in records)
    attrs = client.repository.get_attributes(*paths)

    for record in records:
        if attrs.get(str(record.path), {}).get("filter") == "lfs":
            record.is_lfs = True
        else:
            record.is_lfs = False


@inject.autoparams()
def _get_lfs_file_sizes(records, client_dispatcher: IClientDispatcher):
    """Try to get file size from Git LFS."""
    from humanize import naturalsize  # Slow import

    client = client_dispatcher.current_client

    lfs_files_sizes = {}

    try:
        lfs_run = run(
            ("git", "lfs", "ls-files", "--name-only", "--size"), stdout=PIPE, cwd=client.path, universal_newlines=True
        )
    except SubprocessError:
        pass
    else:
        lfs_output = lfs_run.stdout.split("\n")
        # Example line format: relative/path/to/file (7.9 MB)
        pattern = re.compile(r"^(.*?)\s*\((.*)\)")

        for line in lfs_output:
            match = pattern.search(line)
            if not match:
                continue
            path, size = match.groups()
            # Fix alignment for bytes
            if size.endswith(" B"):
                size = size.replace(" B", "  B")
            lfs_files_sizes[path] = size

    non_lfs_files_sizes = {
        o.path: o.size for o in client.repository.head.commit.traverse() if o.path not in lfs_files_sizes
    }
    non_lfs_files_sizes = {k: naturalsize(v).upper().replace("BYTES", " B") for k, v in non_lfs_files_sizes.items()}

    for record in records:
        size = lfs_files_sizes.get(record.path) or non_lfs_files_sizes.get(record.path)
        record.size = size


def jsonld(records, **kwargs):
    """Format dataset files as JSON-LD.

    :param records: Filtered collection.
    """
    from renku.core.models.json import dumps

    data = [DatasetFileSchema(flattened=True).dump(record) for record in records]
    return dumps(data, indent=2)


def json(records, **kwargs):
    """Format dataset files as JSON.

    :param records: Filtered collection.
    """
    from renku.core.models.dataset import DatasetFileDetailsJson
    from renku.core.models.json import dumps

    _get_lfs_file_sizes(records)
    _get_lfs_tracking(records)

    for record in records:
        record.creators = record.dataset.creators

    data = [DatasetFileDetailsJson().dump(record) for record in records]
    return dumps(data, indent=2)


DATASET_FILES_FORMATS = {
    "tabular": tabular,
    "json-ld": jsonld,
    "json": json,
}
"""Valid formatting options."""

DATASET_FILES_COLUMNS = {
    "added": ("date_added", "added"),
    "commit": ("entity.checksum", "commit"),
    "creators": ("creators_csv", "creators"),
    "creators_full": ("creators_full_csv", "creators"),
    "dataset": ("title", "dataset"),
    "full_path": ("full_path", None),
    "path": ("path", None),
    "short_name": ("dataset_name", "dataset name"),
    "dataset_name": ("dataset_name", "dataset name"),
    "size": ("size", None),
    "lfs": ("is_lfs", "lfs"),
}

DATASET_FILES_COLUMNS_ALIGNMENTS = {"size": "right"}
