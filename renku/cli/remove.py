# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Remove a file, a directory, or a symlink.

Removing a file that belongs to a dataset will update its metadata. It also
will attempt to update tracking information for files stored in an external
storage (using Git LFS).
"""

from pathlib import Path
from subprocess import run

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import WARNING, progressbar


@click.command(name='rm')
@click.argument('sources', type=click.Path(exists=True), nargs=-1)
@pass_local_client(
    clean=True,
    commit=True,
)
@click.pass_context
def remove(ctx, client, sources):
    """Remove files and check repository for potential problems."""
    from renku.core.management.git import _expand_directories

    def fmt_path(path):
        """Format path as relative to the client path."""
        return str(Path(path).absolute().relative_to(client.path))

    files = {
        fmt_path(source): fmt_path(file_or_dir)
        for file_or_dir in sources
        for source in _expand_directories((file_or_dir, ))
    }

    # 1. Update dataset metadata files.
    with progressbar(
        client.datasets.values(),
        item_show_func=lambda item: str(item.short_id) if item else '',
        label='Updating dataset metadata',
        width=0,
    ) as bar:
        for dataset in bar:
            remove = []
            for file_ in dataset.files:
                key = file_.path
                filepath = fmt_path(file_.path)
                if filepath in files:
                    remove.append(key)

            if remove:
                for key in remove:
                    dataset.unlink_file(key)

                dataset.to_yaml()

    # 2. Manage .gitattributes for external storage.
    if client.has_external_storage:
        tracked = tuple(
            path for path, attr in client.find_attr(*files).items()
            if attr.get('filter') == 'lfs'
        )
        client.untrack_paths_from_storage(*tracked)
        existing = client.find_attr(*tracked)
        if existing:
            click.echo(WARNING + 'There are custom .gitattributes.\n')
            if click.confirm(
                'Do you want to edit ".gitattributes" now?', default=False
            ):
                click.edit(filename=str(client.path / '.gitattributes'))

    # Finally remove the files.
    final_sources = list(set(files.values()))
    if final_sources:
        run(['git', 'rm', '-rf'] + final_sources, check=True)
