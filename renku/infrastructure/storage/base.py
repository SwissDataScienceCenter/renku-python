# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Base storage handler."""

import os
import subprocess
from pathlib import Path
from typing import Any, List, Union

from renku.core import errors
from renku.core.interface.storage import IStorage
from renku.core.util.util import NO_VALUE


class RCloneBaseStorage(IStorage):
    """Base external storage handler class."""

    def copy(self, source: Union[Path, str], destination: Union[Path, str]) -> None:
        """Copy data from ``source`` to ``destination``."""
        self.set_configurations()
        execute_rclone_command("copyto", source, destination)

    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
        self.set_configurations()

        try:
            execute_rclone_command("lsf", uri, max_depth=1)
        except errors.StorageObjectNotFound:
            return False
        else:
            return True

    def set_configurations(self) -> None:
        """Set required configurations for rclone to access the storage."""
        for name, value in self.credentials.items():
            name = get_rclone_env_var_name(self.provider.name, name)
            if value is not NO_VALUE:
                set_rclone_env_var(name=name, value=value)


def execute_rclone_command(command: str, *args: Any, **kwargs) -> str:
    """Execute an R-clone command."""
    try:
        result = subprocess.run(
            ("rclone", "--config", "''", command, *transform_kwargs(**kwargs), *transform_args(*args)),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except FileNotFoundError:
        raise errors.RCloneException("RClone is not installed. See https://rclone.org/install/")

    # See https://rclone.org/docs/#list-of-exit-codes for rclone exit codes
    if result.returncode == 0:
        return result.stdout
    if result.returncode in (3, 4):
        raise errors.StorageObjectNotFound(result.stdout)
    elif "AccessDenied" in result.stdout:
        raise errors.AuthenticationError("Authentication failed when accessing the remote storage")
    else:
        raise errors.RCloneException(f"Remote storage operation failed: {result.stdout}")


def transform_args(*args) -> List[str]:
    """Transforms args to command line args."""
    return [str(a) for a in args]


def transform_kwargs(**kwargs) -> List[str]:
    """Transforms kwargs to command line args."""

    def transform_kwarg(key: str, value: Any) -> List[str]:
        if value in (False, None):
            return []
        else:
            name = f"-{key}" if len(key) == 1 else f"--{key.replace('_', '-')}"
            return [name] if value is True else [name, f"{value}"]

    all_args = []
    for key, value in kwargs.items():
        args = transform_kwarg(key, value)
        all_args.extend(args)

    return all_args


def get_rclone_env_var_name(provider_name, name) -> str:
    """Get name of an RClone env var config."""
    # See https://rclone.org/docs/#config-file
    name = name.replace(" ", "_").replace("-", "_")
    return f"RCLONE_CONFIG_{provider_name}_{name}".upper()


def set_rclone_env_var(name, value) -> None:
    """Set value for an RClone config env var."""
    os.environ[name] = value
