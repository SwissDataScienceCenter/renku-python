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
"""Pointer file business logic."""

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple, Union, cast

from renku.core import errors
from renku.core.util.os import is_subpath
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository

if TYPE_CHECKING:
    from renku.domain_model.dataset import DatasetFile


def create_pointer_file(target: Union[str, Path], checksum: Optional[str] = None):
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


def is_linked_file_updated(path: Union[Path, str]) -> Tuple[bool, str]:
    """Check if an update to a linked file is available."""
    pointer_file = get_pointer_file(path=path)

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


def update_linked_file(path: Union[Path, str], checksum: Optional[str]):
    """Delete existing linked file and create a new one."""
    pointer_file = get_pointer_file(path=path)
    target = pointer_file.resolve()

    os.remove(pointer_file)
    absolute_path = project_context.path / path
    os.remove(absolute_path)

    create_external_file(target=target, path=absolute_path, checksum=checksum)


def create_external_file(target: Path, path: Union[Path, str], checksum: Optional[str] = None):
    """Create a new external file."""
    try:
        pointer_file = create_pointer_file(target=target, checksum=checksum)
        relative = os.path.relpath(pointer_file, Path(path).parent)
        os.symlink(relative, path)
    except OSError as e:
        raise errors.OperationError("Could not create symbolic link") from e


def get_pointer_file(path: Union[Path, str]) -> Path:
    """Return pointer file from an external file."""
    absolute_path = project_context.path / path
    link = absolute_path.parent / os.readlink(absolute_path)
    return project_context.path / link


def delete_external_file(dataset_file: "DatasetFile"):
    """Delete an external file."""
    if not dataset_file.is_external or not dataset_file.linked:
        return

    try:
        pointer_file = get_pointer_file(dataset_file.entity.path)
        if os.path.lexists(pointer_file):
            pointer_file.unlink()

        path = project_context.path / dataset_file.entity.path
        if os.path.lexists(path):
            path.unlink()
    except OSError as e:
        raise errors.InvalidFileOperation(f"Cannot remove file '{dataset_file.entity.path}'") from e
