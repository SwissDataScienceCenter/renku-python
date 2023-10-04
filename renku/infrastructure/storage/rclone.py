# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
import posixpath
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Union

from renku.core import errors
from renku.core.interface.storage import FileHash, IStorage
from renku.domain_model.constant import NO_VALUE


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
            hash_type(str): Type of hash to get from rclone (Default value = ``md5``).

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
        hashes = self.list_files(
            uri=uri, hash=True, recursive=True, files_only=True, no_modtime=True, no_mimetype=True, hash_type=hash_type
        )
        output = []
        is_directory = self.is_directory(uri)
        for hash in hashes:
            path: str = hash["Path"]
            full_uri = posixpath.join(uri, path.strip("/")) if is_directory else uri
            hash_content = hash.get("Hashes", {}).get(hash_type)
            file = FileHash(uri=full_uri, path=hash["Path"], size=hash.get("Size"), hash=hash_content)
            output.append(file)

        return output

    def is_directory(self, uri: str) -> bool:
        """Return True if URI points to a directory.

        NOTE: This returns True for non-existing paths on bucket-based backends like S3 since listing non-existing paths
        won't fail and there is no way to distinguish between empty directories and non-existing paths.
        """
        uri = uri.rstrip("/")
        try:
            # NOTE: Listing with a trailing slash works for directories
            files = self.list_files(uri=f"{uri}/")
        except errors.StorageObjectNotFound:
            return False
        else:
            # NOTE: Listing with trailing slash won't fail for non-existing directories on S3 and similar backends
            # NOTE: Listing a file returns exactly one entry in the list; also, if a single entry is a directory then
            # its parent is a directory too.
            if len(files) != 1 or files[0]["IsDir"]:
                return True
            pathname = files[0]["Path"]
            if pathname != posixpath.basename(uri):
                return True
            # NOTE: The only remaining possibility is a directory with a single file with the same name (e.g. data/data:
            # Listing data/ and data/data returns the same results)
            try:
                files = self.list_files(uri=f"{uri}/{pathname}")
            except (errors.StorageObjectNotFound, errors.ParameterError):
                return False
            else:
                return True if len(files) == 1 else False

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

    def list_files(self, uri: str, *args, **kwargs) -> List[Dict[str, Any]]:
        """List a URI and return results in JSON format."""
        hashes_raw = self.run_command_with_uri("lsjson", uri, *args, **kwargs)
        try:
            hashes = json.loads(hashes_raw)
        except json.JSONDecodeError as e:
            raise errors.RCloneException(f"Cannot parse command output: {e}")
        if not hashes:
            raise errors.ParameterError(f"Cannot find URI: {uri}")

        return hashes

    def run_command_with_uri(self, command: str, uri: str, *args, **kwargs) -> Any:
        """Run a RClone command by converting a given URI."""
        uri = self.provider.convert_to_storage_uri(uri)

        return self.run_command(command, uri, *args, **kwargs)

    def run_command(self, command: str, *args, **kwargs) -> Any:
        """Run a RClone command with storage-specific configuration."""
        return run_rclone_command(command, *args, **kwargs, env=self.get_configurations())

    def upload(self, source: Union[Path, str], uri: str) -> None:
        """Upload data from ``source`` to ``uri``."""
        uri = self.provider.convert_to_storage_uri(uri)

        self.run_command("copyto", source, uri)


def run_rclone_command(command: str, *args: Any, env=None, **kwargs) -> str:
    """Execute an RClone command."""
    os_env = os.environ.copy()
    if env:
        os_env.update(env)

    full_command = ("rclone", "--config", os.devnull, command, *transform_kwargs(**kwargs), *transform_args(*args))
    try:
        result = subprocess.run(full_command, text=True, capture_output=True, env=os_env)
    except FileNotFoundError:
        raise errors.RCloneException("RClone is not installed. See https://rclone.org/install/")

    # See https://rclone.org/docs/#list-of-exit-codes for rclone exit codes
    if result.returncode == 0:
        return result.stdout

    all_outputs = result.stdout + result.stderr
    if (
        result.returncode in (3, 4)
        or (result.returncode == 1 and "failed to read directory entry" in all_outputs)
        or (result.returncode == 1 and "no Host in request URL" in all_outputs)
    ):
        raise errors.StorageObjectNotFound(all_outputs)
    elif (
        "AccessDenied" in all_outputs
        or "no authentication method configured" in all_outputs
        or "Need account+key" in all_outputs
    ):
        raise errors.AuthenticationError(f"Authentication failed when accessing the cloud storage: {all_outputs}")
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
