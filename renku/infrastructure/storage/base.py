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

import json
import os
import subprocess
from pathlib import Path
from typing import Any, List, Union

from renku.core import errors
from renku.core.interface.storage import FileHash, IStorage
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

    def get_hashes(self, uri: str, hash_type: str = "md5") -> List[FileHash]:
        """Download hashes with rclone and parse them.

        Returns a tuple containing a list of parsed hashes.

        Example raw_hashes json:
        [
            {
                "Path":"resources/hg19.windowmaskerSdust.bed.gz.tbi","Name":"hg19.windowmaskerSdust.bed.gz.tbi",
                "Size":578288,"MimeType":"application/x-gzip","ModTime":"2022-02-07T18:45:52.000000000Z",
                "IsDir":false,"Hashes":{"md5":"e93ac5364e7799bbd866628d66c7b773"},"Tier":"STANDARD"
            }
        ]
        """
        self.set_configurations()

        hashes_raw = execute_rclone_command("lsjson", "--hash", "-R", "--files-only", uri)
        hashes = json.loads(hashes_raw)
        if not hashes:
            raise errors.ParameterError(f"Cannot find URI: {uri}")

        output = []
        for hash in hashes:
            hash_content = hash.get("Hashes", {}).get(hash_type)
            output.append(
                FileHash(
                    base_uri=uri,
                    path=hash["Path"],
                    hash=hash_content,
                    hash_type=hash_type if hash_content else None,
                    modified_datetime=hash.get("ModTime"),
                )
            )
        return output

    def mount(self, path: Union[Path, str]) -> None:
        """Mount the provider's URI to the given path."""
        self.set_configurations()
        execute_rclone_command("mount", self.provider.uri, path, daemon=True, read_only=True, no_modtime=True)

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
            ("rclone", command, *transform_kwargs(**kwargs), *args),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise errors.RCloneException("RClone is not installed. See https://rclone.org/install/")

    # See https://rclone.org/docs/#list-of-exit-codes for rclone exit codes
    if result.returncode == 0:
        return result.stdout

    all_outputs = result.stdout + result.stderr
    if result.returncode in (3, 4):
        raise errors.StorageObjectNotFound(all_outputs)
    elif "AccessDenied" in all_outputs:
        raise errors.AuthenticationError("Authentication failed when accessing the remote storage")
    else:
        raise errors.RCloneException(f"Remote storage operation failed: {all_outputs}")


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
