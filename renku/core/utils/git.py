# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Git utility functions."""

import math
from subprocess import SubprocessError, run

from renku.core import errors

ARGUMENT_BATCH_SIZE = 100


def run_command(command, *paths, separator=None, **kwargs):
    """Execute command by splitting `paths` to make sure that argument list will be within os limits.

    :param command: A list or tuple containing command and its arguments.
    :param paths: List of paths/long argument. This will be appended to `command` for each invocation.
    :param separator: Separator for `paths` if they need to be passed as string.
    :param kwargs: Extra arguments passed to `subprocess.run`.
    :returns: Result of last invocation.
    """
    result = None

    for batch in split_paths(*paths):
        if separator:
            batch = [separator.join(batch)]

        try:
            if not isinstance(batch, list):
                batch = list(batch)
            result = run(command + batch, **kwargs)

            if result.returncode != 0:
                break
        except KeyboardInterrupt:
            raise
        except SubprocessError as e:
            raise errors.GitError(f"Cannot run command {command} : {e}")

    return result


def add_to_git(git, *paths, **kwargs):
    """Split `paths` and add them to git to make sure that argument list will be within os limits."""
    for batch in split_paths(*paths):
        git.add(*batch, **kwargs)


def split_paths(*paths):
    """Return a generator with split list of paths."""
    batch_count = math.ceil(len(paths) / ARGUMENT_BATCH_SIZE)
    batch_count = max(batch_count, 1)

    for index in range(batch_count):
        yield paths[index * ARGUMENT_BATCH_SIZE : (index + 1) * ARGUMENT_BATCH_SIZE]
