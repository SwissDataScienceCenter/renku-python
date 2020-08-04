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
"""Check for required Git hooks."""
from io import StringIO
from pathlib import Path

import pkg_resources
from git.index.fun import hook_path as get_hook_path

from renku.core.management.githooks import HOOKS

from ..echo import WARNING


def check_git_hooks_installed(client):
    """Checks if all necessary hooks are installed."""
    for hook in HOOKS:
        hook_path = Path(get_hook_path(hook, client.repo.git_dir))
        if not hook_path.exists():
            message = WARNING + "Git hooks are not installed. " 'Use "renku githooks install" to install them. \n'
            return False, message

        with hook_path.open() as file_:
            actual_hook = _extract_renku_hook(file_)
        with StringIO(_read_resource(hook)) as file_:
            expected_hook = _extract_renku_hook(file_)

        if not expected_hook:
            message = WARNING + "Cannot check for existence of Git hooks.\n"
            return False, message

        if actual_hook != expected_hook:
            message = (
                WARNING + "Git hooks are outdated or not installed.\n"
                '  (use "renku githooks install --force" to update them) \n'
            )
            return False, message

    return True, None


def _extract_renku_hook(file_):
    lines = [line.strip() for line in file_ if line.strip()]
    start = end = -1
    for index, line in enumerate(lines):
        if line.startswith("# RENKU HOOK."):
            start = index
        elif line.endswith("# END RENKU HOOK."):
            end = index
            break

    return lines[start:end] if 0 <= start <= end else []


def _read_resource(hook):
    return pkg_resources.resource_string("renku.data", "{hook}.sh".format(hook=hook)).decode("utf-8")
