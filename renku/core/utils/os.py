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
"""OS utility functions."""

import hashlib
import os
import re
from pathlib import Path
from typing import Generator, List, Optional, Union

from renku.core import errors

BLOCK_SIZE = 4096


def get_relative_path_to_cwd(path: Union[Path, str]) -> str:
    """Get a relative path to current working directory."""
    absolute_path = os.path.abspath(path)
    return os.path.relpath(absolute_path, os.getcwd())


def get_absolute_path(path: Union[Path, str], base: Union[Path, str] = None) -> str:
    """Return absolute normalized path without resolving symlinks."""
    if base is not None:
        base = Path(base).resolve()
        path = os.path.join(base, path)

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


def get_relative_path(path: Union[Path, str], base: Union[Path, str], strict: bool = False) -> Optional[Path]:
    """Return a relative path to the base if path is within base without resolving symlinks."""
    try:
        absolute_path = get_absolute_path(path=path, base=base)
        return Path(absolute_path).relative_to(base)
    except ValueError:
        if strict:
            raise errors.ParameterError("File {} is not within path {}".format(path, base))
        return


def is_subpath(path: Union[Path, str], base: Union[Path, str]) -> bool:
    """Return True if path is within base."""
    return get_relative_path(path, base) is not None


def get_relative_paths(base: Union[Path, str], paths: List[Union[Path, str]]) -> List[str]:
    """Return a list of paths relative to a base path."""
    relative_paths = []

    for path in paths:
        relative_path = get_relative_path(path=path, base=base)
        if relative_path is None:
            raise errors.ParameterError(f"Path '{path}' is not within base path '{base}'")

        relative_paths.append(str(relative_path))

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


def is_path_empty(path: Union[Path, str]) -> bool:
    """Check if path contains files.

    :ref path: target path
    """
    subpaths = Path(path).rglob("*")
    return not any(subpaths)


def print_markdown(text: str):
    """Print markdown text to console."""
    from rich.console import Console
    from rich.markdown import Markdown

    Console().print(Markdown(text))


def is_ascii(data):
    """Check if provided string contains only ascii characters."""
    return len(data) == len(data.encode())


def normalize_to_ascii(input_string, sep="-"):
    """Adjust chars to make the input compatible as scm source."""
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


def delete_file(filepath: Union[Path, str], ignore_errors: bool = True, follow_symlinks: bool = False):
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


def hash_file(path: Union[Path, str]) -> Optional[str]:
    """Calculate the sha256 hash of a file."""
    if not os.path.exists(path):
        return

    sha256_hash = hashlib.sha256()

    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(BLOCK_SIZE), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def hash_str(content: str):
    """Calculate the sha256 hash of a string."""
    sha256_hash = hashlib.sha256()

    content_bytes = content.encode("utf-8")

    blocks = (len(content_bytes) - 1) // BLOCK_SIZE + 1
    for i in range(blocks):
        byte_block = content_bytes[i * BLOCK_SIZE : (i + 1) * BLOCK_SIZE]
        sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()
