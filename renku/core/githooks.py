# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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

from pydantic import validate_arguments

from renku.core.util.git import get_hook_path
from renku.domain_model.project_context import project_context

try:
    import importlib_resources  # type:ignore
except ImportError:
    import importlib.resources as importlib_resources  # type: ignore

HOOKS = ("pre-commit",)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def install_githooks(force: bool, path: Path):
    """Install Git hooks."""
    warning_messages = []
    for hook in HOOKS:
        hook_path = get_hook_path(name=hook, path=path)
        if hook_path.exists():
            if not force:
                warning_messages.append(f"Hook already exists. Skipping {hook_path}")
                continue
            else:
                hook_path.unlink()

        # Make sure the hooks directory exists.
        hook_path.parent.mkdir(parents=True, exist_ok=True)
        hook_data = importlib_resources.files("renku.data").joinpath(f"{hook}.sh").read_bytes()
        Path(hook_path).write_bytes(hook_data)
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)

    return warning_messages


def uninstall_githooks():
    """Uninstall Git hooks."""
    for hook in HOOKS:
        hook_path = get_hook_path(name=hook, path=project_context.path)
        if hook_path.exists():
            hook_path.unlink()
