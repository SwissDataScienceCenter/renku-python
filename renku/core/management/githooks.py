# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Install and uninstall Git hooks."""

import stat
from pathlib import Path

import pkg_resources
from git.index.fun import hook_path as get_hook_path

HOOKS = ('pre-commit', )


def install(client, force):
    """Install Git hooks."""
    warning_messages = []
    for hook in HOOKS:
        hook_path = Path(get_hook_path(hook, client.repo.git_dir))
        if hook_path.exists():
            if not force:
                warning_messages.append(
                    'Hook already exists. Skipping {0}'.format(str(hook_path))
                )
                continue
            else:
                hook_path.unlink()

        # Make sure the hooks directory exists.
        hook_path.parent.mkdir(parents=True, exist_ok=True)

        Path(hook_path).write_bytes(
            pkg_resources.resource_string(
                'renku.data', '{hook}.sh'.format(hook=hook)
            )
        )
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)

    return warning_messages


def uninstall(client):
    """Uninstall Git hooks."""
    for hook in HOOKS:
        hook_path = Path(get_hook_path(hook, client.repo.git_dir))
        if hook_path.exists():
            hook_path.unlink()
