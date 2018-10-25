# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Pull data from LFS."""

import click

from ._client import pass_local_client


@click.group()
def pull():
    """Pull data from LFS."""


@pull.command()
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=True),
    nargs=-1,
    required=True,
)
@pass_local_client
def file(client, filename):
    """Create an empty dataset in the current repo."""
    from pathlib import Path

    # handle a symlink
    # 1. check if the filename is a symlink
    # 2. if yes, check if it is inside a submodule
    # 3. if no, make sure the target is inside the current project and pull
    # 4. if in a submodule, go there and pull

    p = Path(filename)
    if p.is_symlink():
        p = p.resolve()
        submodule_path = in_submodule(p, client.git)
        if submodule_path:
            lfs_pull_file(p.relative_to(submodule_path), cwd=submodule_path)

        else:
            # TODO: check if the file is inside the project
            lfs_pull_file(
                p.relative_to(Path(client.git.working_dir).absolute()),
                cwd=client.git.working_dir
            )

    else:
        lfs_pull_file(p)


def in_submodule(path, repo):
    """Check if a path is inside a submodule."""
    from pathlib import Path
    for submodule in repo.submodules:
        submodule_path = Path(submodule.path).absolute()
        if str(path).startswith(str(submodule_path)):
            return submodule_path


def lfs_pull_file(path, cwd=None):
    """Pull a path from LFS."""
    import subprocess
    subprocess.run(['git', 'lfs', 'pull', 'origin', '-I',
                    str(path)],
                   cwd=cwd,
                   check=True)
