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
"""Pointer file business logic."""

import os
import uuid
from pathlib import Path
from typing import Optional, Tuple, Union, cast

from renku.core import errors
from renku.core.util.os import is_subpath
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository


def create_pointer_file(target: Union[str, Path], checksum: str = None):
    """Create a new pointer file."""
    target = Path(target).resolve()

    if checksum is None:
        checksum = Repository.hash_object(target)
        assert checksum is not None, f"Cannot calculate checksum for '{target}'"

    while True:
        filename = f"{uuid.uuid4()}-{checksum}"
        path = project_context.pointers_path / filename
        if not path.exists():
            break

    # NOTE: If target is within the repo, add it as a relative symlink
    is_within_repo = is_subpath(target, base=project_context.path)
    source = cast(Union[str, bytes, Path], os.path.relpath(target, path.parent) if is_within_repo else target)

    try:
        os.symlink(source, path)
    except FileNotFoundError:
        raise errors.ExternalFileNotFound(target)

    return path


def is_external_file_updated(project_path: Path, path: Union[Path, str]) -> Tuple[bool, str]:
    """Check if an update to an external file is available."""
    pointer_file = get_pointer_file(project_path=project_path, path=path)

    try:
        target = pointer_file.resolve(strict=True)
    except FileNotFoundError:
        target = pointer_file.resolve()
        raise errors.ExternalFileNotFound(target)

    new_checksum = Repository.hash_object(target)
    old_checksum = pointer_file.name.split("-")[-1]

    if new_checksum is None:
        raise errors.ExternalFileNotFound(target)

    updated = new_checksum != old_checksum

    return updated, new_checksum


def update_external_file(path: Union[Path, str], checksum: Optional[str]):
    """Delete existing external file and create a new one."""
    pointer_file = get_pointer_file(project_path=project_context.path, path=path)
    target = pointer_file.resolve()

    os.remove(pointer_file)
    absolute_path = project_context.path / path
    os.remove(absolute_path)

    create_external_file(target=target, path=absolute_path, checksum=checksum)


def create_external_file(target: Path, path: Union[Path, str], checksum: str = None):
    """Create a new external file."""
    try:
        pointer_file = create_pointer_file(target=target, checksum=checksum)
        relative = os.path.relpath(pointer_file, Path(path).parent)
        os.symlink(relative, path)
    except OSError as e:
        raise errors.OperationError("Could not create symbolic link") from e


def get_pointer_file(project_path: Path, path: Union[Path, str]) -> Path:
    """Return pointer file from an external file."""
    absolute_path = project_path / path
    link = absolute_path.parent / os.readlink(absolute_path)
    return project_path / link
