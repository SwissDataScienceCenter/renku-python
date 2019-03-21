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
"""Move or rename a file, a directory, or a symlink.

Moving a file that belongs to a dataset will update its metadata. It also
will attempt to update tracking information for files stored in an external
storage (using Git LFS). Finally it makes sure that all relative symlinks work
after the move.
"""

import os
from subprocess import run

import click

from renku._compat import Path

from ._client import pass_local_client
from ._echo import WARNING, progressbar


@click.command(name='mv')
@click.argument('sources', type=click.Path(exists=True), nargs=-1)
@click.argument('destination', type=click.Path(), nargs=1)
@pass_local_client(clean=True, commit=True)
@click.pass_context
def move(ctx, client, sources, destination):
    """Move files and check repository for potential problems."""
    from renku.api._git import _expand_directories

    dst = Path(destination)

    def fmt_path(path):
        """Format path as relative to the client path."""
        return str(Path(path).absolute().relative_to(client.path))

    files = {
        fmt_path(source): fmt_path(file_or_dir)
        for file_or_dir in sources
        for source in _expand_directories((file_or_dir, ))
    }

    def fmt_dst(path):
        """Build a destination path for a source path."""
        return str(dst / os.path.relpath(path, start=files[path]))

    destinations = {source: fmt_dst(source) for source in files}

    # 1. Check .gitignore.
    ignored = client.find_ignored_paths(*destinations.values())
    if ignored:
        click.echo(WARNING + 'Renamed files match .gitignore.\n')
        if click.confirm(
            'Do you want to edit ".gitignore" now?', default=False
        ):
            click.edit(filename=str(client.path / '.gitignore'))

    # 2. Update dataset metadata files.
    with progressbar(
        client.datasets.items(),
        item_show_func=lambda item: str(item[1].short_id) if item else '',
        label='Updating dataset metadata',
        width=0,
    ) as bar:
        for (path, dataset) in bar:
            renames = {}

            for file in dataset.files:
                filepath = fmt_path(os.path.normpath(str(path.parent / file)))

                if filepath in files:
                    renames[file] = os.path.relpath(
                        destinations[filepath], start=str(path.parent)
                    )

            if renames:
                dataset = dataset.rename_files(
                    lambda key: renames.get(key, key)
                )

                client.store_dataset(dataset)

    # 3. Manage .gitattributes for external storage.
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

    client.track_paths_in_storage(*(destinations[path] for path in tracked))

    # 4. Handle symlinks.
    dst.parent.mkdir(parents=True, exist_ok=True)

    for source, target in destinations.items():
        src = Path(source)
        if src.is_symlink():
            Path(target).parent.mkdir(parents=True, exist_ok=True)
            Path(target).symlink_to(
                os.path.relpath(
                    str(src.resolve()), start=os.path.dirname(target)
                )
            )
            src.unlink()
            del files[source]

    # Finally move the files.
    final_sources = list(set(files.values()))
    if final_sources:
        run(['git', 'mv'] + final_sources + [destination], check=True)
