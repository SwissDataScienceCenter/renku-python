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
"""Check for large files in Git history."""
import re
import shlex
from subprocess import PIPE, STDOUT, run

from renku.core import errors
from renku.core.commands.echo import WARNING


def check_lfs_info(client):
    """Checks if files in history should be in LFS."""
    if not client.has_external_storage:
        return True, None

    exclude_patterns = _read_lfs_exclude_patterns(client)
    above_size = client.minimum_lfs_file_size

    excludes = [
        p for e in exclude_patterns for p in ('--exclude', shlex.quote(e))
    ]

    lfs_migrate_cmd = [
        'git', 'lfs', 'migrate', 'info', '--include-ref',
        client.repo.active_branch.name, '--above',
        str(above_size), '--top', '42000'
    ] + excludes

    try:
        lfs_output = run(
            lfs_migrate_cmd,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=client.path,
            universal_newlines=True
        )
    except (KeyboardInterrupt, OSError) as e:
        raise errors.GitError(
            'Couldn\'t run \'git lfs migrate info\':\n{0}'.format(e)
        )

    files = []
    files_re = re.compile(r'(.*\s+[\d.]+\s+\S+).*')

    for line in lfs_output.stdout.split('\n'):
        match = files_re.match(line)
        if match:
            files.append(match.groups()[0])

    if not files:
        return True, None

    message = (
        WARNING + 'Git history contains files that should be in LFS.\n\t' +
        '\n\t'.join(files) + '\n'
    )

    return False, message


def _read_lfs_exclude_patterns(client):
    lfs_ignore_path = client.path / client.RENKU_LFS_IGNORE_PATH

    try:
        lines = open(lfs_ignore_path).read().split('\n')
    except FileNotFoundError:
        return []

    return [l for l in lines if l.strip() and not l.strip().startswith('#')]
