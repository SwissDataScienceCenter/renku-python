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

import concurrent.futures
import json
import os
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any, List

from renku.core import errors
from renku.core.interface.storage import FileHash, IStorage
from renku.core.util.util import NO_VALUE


class RCloneBaseStorage(IStorage):
    """Base external storage handler class."""

    def set_configurations(self):
        """Set required configurations for rclone to access the storage."""
        for name, value in self.credentials.items():
            name = get_rclone_env_var_name(self.provider.name, name)
            if value is not NO_VALUE:
                set_rclone_env_var(name=name, value=value)

    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
        self.set_configurations()

        try:
            execute_rclone_command("lsf", uri, max_depth=1)
        except errors.StorageObjectNotFound:
            return False
        else:
            return True

    def mount(self, uri: str, mount_location: Path):
        """Mount the path from the uri to a specific location locally."""
        if not mount_location.exists():
            raise errors.DirectoryNotFound(mount_location)
        if not mount_location.is_dir():
            raise errors.ExpectedDirectoryGotFile(mount_location)
        if os.path.ismount(mount_location):
            raise errors.ExpectedDirectoryGotMountPoint(mount_location)
        if next(mount_location.iterdir(), None):
            raise errors.DirectoryNotEmptyError(mount_location)
        if not self.exists(uri):
            raise errors.StorageObjectNotFound
        self.set_configurations()
        execute_rclone_command("mount", "--daemon", uri, str(mount_location.absolute()))

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
        output = self._get_missing_hashes(output, hash_type=hash_type)
        return output

    def _get_missing_hashes(self, hashes: List[FileHash], hash_type: str = "md5") -> List[FileHash]:
        """Go through the list of hashes and compute any hashes that are missing.

        This can be a very slow operation for large files. Rclone will download the files and
        compute the hashes. But this takes time and resources. S3 computes hashes automatically
        for many files but usually not for *.gz or other compressed files. This will fill in any
        missing hashes in the list.
        """

        def _compute_hash(hash: FileHash, hash_type: str) -> FileHash:
            self.set_configurations()
            res = execute_rclone_command("hashsum", hash_type, "--download", hash.base_uri)
            return FileHash(**asdict(hash), hash=res.split()[0])

        missing_hashes = []
        valid_hashes = []
        for hash in hashes:
            if hash.hash:
                valid_hashes.append(hash)
            else:
                missing_hashes.append(hash)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(_compute_hash, hash, "md5") for hash in missing_hashes]
            computed_hashes = [future.result() for future in futures]

        return [*valid_hashes, *computed_hashes]


def execute_rclone_command(command: str, *args: str, **kwargs) -> str:
    """Execute an R-clone command."""
    try:
        result = subprocess.run(
            ("rclone", command, *transform_kwargs(**kwargs), *args),
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
    # RCLONE_CONFIG_MYS3_TYPE
    name = name.replace(" ", "_").replace("-", "_")
    return f"RCLONE_CONFIG_{provider_name}_{name}".upper()


def set_rclone_env_var(name, value) -> None:
    """Set value for an RClone config env var."""
    os.environ[name] = value
