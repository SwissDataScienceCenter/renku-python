# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""OS utility functions."""

import fnmatch
import glob
import hashlib
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, BinaryIO, Dict, Generator, List, Optional, Sequence, Union

from renku.core import errors

BLOCK_SIZE = 4096


def get_relative_path_to_cwd(path: Union[Path, str]) -> str:
    """Get a relative path to current working directory."""
    absolute_path = os.path.abspath(path)
    return os.path.relpath(absolute_path, os.getcwd())


def get_absolute_path(path: Union[Path, str], base: Union[Path, str] = None, resolve_symlinks: bool = False) -> str:
    """Return absolute normalized path."""
    if base is not None:
        base = Path(base).resolve() if resolve_symlinks else os.path.abspath(base)
        path = os.path.join(base, path)

    if resolve_symlinks:
        return os.path.realpath(path)
    else:
        # NOTE: Do not use os.path.realpath or Path.resolve() because they resolve symlinks
        return os.path.abspath(path)


def get_safe_relative_path(path: Union[Path, str], base: Union[Path, str]) -> Path:
    """Return a relative path to the base and check path is within base with all symlinks resolved.

    NOTE: This is used to prevent path traversal attack.
    """
    try:
        base = Path(base).resolve()
        absolute_path = base / path
        return absolute_path.resolve().relative_to(base)
    except ValueError:
        raise ValueError(f"Path '{path}' is not with base directory '{base}'")


def get_relative_path(path: Union[Path, str], base: Union[Path, str], strict: bool = False) -> Optional[str]:
    """Return a relative path to the base if path is within base with/without resolving symlinks."""
    try:
        absolute_path = get_absolute_path(path=path, base=base)
        return str(Path(absolute_path).relative_to(base))
    except ValueError:
        if strict:
            raise errors.ParameterError(f"File {path} is not within path {base}")

    return None


def is_subpath(path: Union[Path, str], base: Union[Path, str]) -> bool:
    """Return True if path is within or same as base."""
    absolute_path = get_absolute_path(path=path)
    absolute_base = get_absolute_path(path=base)
    try:
        Path(absolute_path).relative_to(absolute_base)
    except ValueError:
        return False
    else:
        return True


def get_relative_paths(paths: Sequence[Union[Path, str]], base: Union[Path, str]) -> List[str]:
    """Return a list of paths relative to a base path."""
    relative_paths = []

    for path in paths:
        relative_path = get_relative_path(path=path, base=base)
        if relative_path is None:
            raise errors.ParameterError(f"Path '{path}' is not within base path '{base}'")

        relative_paths.append(relative_path)

    return relative_paths


def get_files(path: Path) -> Generator[Path, None, None]:
    """Return all files from a starting file/directory."""
    if not path.is_dir():
        yield path
    else:
        for subpath in path.rglob("*"):
            if not subpath.is_dir():
                yield subpath


def are_paths_related(a, b) -> bool:
    """Return True if paths are equal or one is the parent of the other."""
    common_path = os.path.commonpath((a, b))
    absolute_common_path = os.path.abspath(common_path)
    return absolute_common_path == os.path.abspath(a) or absolute_common_path == os.path.abspath(b)


def are_paths_equal(a: Union[Path, str], b: Union[Path, str]) -> bool:
    """Returns if two paths are the same."""
    # NOTE: The two paths should be identical; we don't consider the case where one is a sub-path of another
    return get_absolute_path(a) == get_absolute_path(b)


def is_path_empty(path: Union[Path, str]) -> bool:
    """Check if path contains files.

    :ref path: target path
    """
    subpaths = Path(path).glob("*")
    return not any(subpaths)


def create_symlink(path: Union[Path, str], symlink_path: Union[Path, str], overwrite: bool = True) -> None:
    """Create a symlink that points from symlink_path to path."""
    # NOTE: Don't resolve symlink path
    absolute_symlink_path = get_absolute_path(symlink_path)
    absolute_path = get_absolute_path(path, resolve_symlinks=True)

    Path(absolute_symlink_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        if overwrite:
            delete_path(absolute_symlink_path)
        os.symlink(absolute_path, absolute_symlink_path)
    except OSError:
        raise errors.InvalidFileOperation(f"Cannot create symlink from '{symlink_path}' to '{path}'")


def delete_path(path: Union[Path, str]) -> None:
    """Delete a file/directory/symlink."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except (PermissionError, IsADirectoryError, OSError):
        shutil.rmtree(path, ignore_errors=True)


def unmount_path(path: Union[Path, str]) -> None:
    """Unmount the given path and ignore all errors."""

    def execute_command(*command: str) -> bool:
        try:
            subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
        else:
            return True

    path = str(path)

    # NOTE: A symlink means that the path is not mounted itself but it's a link to a mount-point; just delete the link.
    if os.path.islink(path):
        os.remove(path)
        return

    # NOTE: ``fusermount`` is available on linux and ``umount`` is for macOS
    result = False
    if shutil.which("fusermount"):
        result = execute_command("fusermount", "-u", "-z", path)

    if not result:
        execute_command("umount", path)


def is_ascii(data):
    """Check if provided string contains only ascii characters."""
    return len(data) == len(data.encode())


def normalize_to_ascii(input_string, sep="-"):
    """Convert a string to only contain ASCII characters, with non-ASCII substring replaced with ``sep``."""
    replace_all = [sep, "_", "."]
    for replacement in replace_all:
        input_string = input_string.replace(replacement, " ")

    return (
        sep.join(
            [
                component
                for component in re.sub(r"[^a-zA-Z0-9_.-]+", " ", input_string).split(" ")
                if component and is_ascii(component)
            ]
        )
        .lower()
        .strip(sep)
    )


def delete_dataset_file(filepath: Union[Path, str], ignore_errors: bool = True, follow_symlinks: bool = False):
    """Remove a file/symlink and its pointer file (for external files)."""
    path = Path(filepath)
    link = None
    try:
        link = path.parent / os.readlink(path)
    except FileNotFoundError:
        if not ignore_errors:
            raise
        return
    except OSError:  # not a symlink but a normal file
        pass

    try:
        os.remove(path)
    except OSError:
        if not ignore_errors:
            raise

    if follow_symlinks and link:
        try:
            os.remove(link)
        except FileNotFoundError:
            pass


def hash_file(path: Union[Path, str], hash_type: str = "sha256") -> Optional[str]:
    """Calculate the sha256 hash of a file."""
    if not os.path.exists(path):
        return None

    with open(path, "rb") as f:
        return hash_file_descriptor(f, hash_type)


def hash_file_descriptor(file: BinaryIO, hash_type: str = "sha256") -> str:
    """Hash content of a file descriptor."""
    hash_type = hash_type.lower()
    assert hash_type in ("sha256", "md5")

    hash_value = hashlib.sha256() if hash_type == "sha256" else hashlib.md5()

    for byte_block in iter(lambda: file.read(BLOCK_SIZE), b""):
        hash_value.update(byte_block)

    return hash_value.hexdigest()


def safe_read_yaml(path: Union[Path, str]) -> Dict[str, Any]:
    """Parse a YAML file.

    Returns:
        In case of success a dictionary of the YAML's content, otherwise raises a ParameterError exception.
    """
    try:
        from renku.core.util import yaml as yaml

        return yaml.read_yaml(path)
    except Exception as e:
        raise errors.ParameterError(e)


def matches(path: Union[Path, str], pattern: str) -> bool:
    """Check if a path matched a given pattern."""
    pattern = pattern.rstrip(os.sep)

    path = Path(path)
    paths = [path] + list(path.parents)[:-1]

    for parent in paths:
        if fnmatch.fnmatch(str(parent), pattern):
            return True

    return False


def expand_directories(paths):
    """Expand directory with all files it contains."""
    processed_paths = set()
    for path in paths:
        for matched_path in glob.iglob(str(path), recursive=True):
            if matched_path in processed_paths:
                continue
            path_ = Path(matched_path)
            if path_.is_dir():
                for expanded in path_.rglob("*"):
                    processed_paths.add(str(expanded))
                    yield str(expanded)
            else:
                processed_paths.add(matched_path)
                yield matched_path
