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
from typing import Any, Dict, List, Union

from renku.core import errors
from renku.core.interface.storage import FileHash, IStorage
from renku.core.util.util import NO_VALUE


class RCloneStorage(IStorage):
    """External storage implementation that uses RClone."""

    def download(self, uri: str, destination: Union[Path, str]) -> None:
        """Download data from ``uri`` to ``destination``."""
        self.run_command_with_uri("copyto", uri, destination)

    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
        try:
            self.run_command_with_uri("lsf", uri=uri, max_depth=1)
        except errors.StorageObjectNotFound:
            return False
        else:
            return True

    def get_hashes(self, uri: str, hash_type: str = "md5") -> List[FileHash]:
        """Download hashes with rclone and parse them.

        Returns a tuple containing a list of parsed hashes.

        Arguments:
            uri(str): Provider uri.
            hash_type(str): Type of hash to get from rclone (Default value = `md5`).

        Example:
            hashes_raw json::

                [
                    {
                        "Path":"resources/hg19.window.masker.bed.gz.tbi","Name":"hg19.window.masker.bed.gz.tbi",
                        "Size":578288,"MimeType":"application/x-gzip","ModTime":"2022-02-07T18:45:52.000000000Z",
                        "IsDir":false,"Hashes":{"md5":"e93ac5364e7799bbd866628d66c7b773"},"Tier":"STANDARD"
                    }
                ]
        """
        hashes_raw = self.run_command_with_uri("lsjson", uri, hash=True, R=True, files_only=True)
        # TODO: Handle JSON load errors
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
        self.run_command_with_uri("mount", self.provider.uri, str(path), daemon=True, read_only=True, no_modtime=True)

    def get_configurations(self) -> Dict[str, str]:
        """Get required configurations for rclone to access the storage."""
        configurations = {}
        for name, value in self.credentials.items():
            if value is not NO_VALUE:
                name = get_rclone_env_var_name(self.storage_scheme, name)
                configurations[name] = value

        for name, value in self._provider_configuration.items():
            name = get_rclone_env_var_name(self.storage_scheme, name)
            configurations[name] = value

        return configurations

    def run_command_with_uri(self, command: str, uri: str, *args, **kwargs) -> Any:
        """Run a RClone command by converting a given URI."""
        uri = self._provider_uri_convertor(uri)

        return self.run_command(command, uri, *args, **kwargs)

    def run_command(self, command: str, *args, **kwargs) -> Any:
        """Run a RClone command with storage-specific configuration."""
        return run_rclone_command(command, *args, **kwargs, env=self.get_configurations())

    def upload(self, source: Union[Path, str], uri: str) -> None:
        """Upload data from ``source`` to ``uri``."""
        uri = self._provider_uri_convertor(uri)

        self.run_command("copyto", source, uri)


def run_rclone_command(command: str, *args: Any, env=None, **kwargs) -> str:
    """Execute an RClone command."""
    os_env = os.environ.copy()
    if env:
        os_env.update(env)

    full_command = ("rclone", command, *transform_kwargs(**kwargs), *transform_args(*args))
    try:
        result = subprocess.run(full_command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os_env)
    except FileNotFoundError:
        raise errors.RCloneException("RClone is not installed. See https://rclone.org/install/")

    # See https://rclone.org/docs/#list-of-exit-codes for rclone exit codes
    if result.returncode == 0:
        return result.stdout

    all_outputs = result.stdout + result.stderr
    if result.returncode in (3, 4):
        raise errors.StorageObjectNotFound(all_outputs)
    elif "AccessDenied" in all_outputs:
        raise errors.AuthenticationError(f"Authentication failed when accessing the remote storage: {all_outputs}")
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
