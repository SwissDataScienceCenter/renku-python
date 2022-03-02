# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
from typing import TYPE_CHECKING, Union

from renku.core import errors
from renku.core.management.dataset.constant import renku_pointers_path

if TYPE_CHECKING:
    from renku.core.management.client import LocalClient


def create_pointer_file(client: "LocalClient", target: Union[str, Path], checksum: str = None):
    """Create a new pointer file."""
    target = Path(target).resolve()

    if checksum is None:
        checksum = client.repository.hash_object(target)
        assert checksum is not None, f"Cannot calculate checksum for '{target}'"

    while True:
        filename = f"{uuid.uuid4()}-{checksum}"
        path = renku_pointers_path(client) / filename
        if not path.exists():
            break

    try:
        os.symlink(target, path)
    except FileNotFoundError:
        raise errors.ParameterError("Cannot find external file {}".format(target))

    return path


def update_pointer_file(client: "LocalClient", pointer_file_path: Path):
    """Update a pointer file."""
    try:
        target = pointer_file_path.resolve(strict=True)
    except FileNotFoundError:
        target = pointer_file_path.resolve()
        raise errors.ParameterError("External file not found: {}".format(target))

    checksum = client.repository.hash_object(target)
    current_checksum = pointer_file_path.name.split("-")[-1]

    if checksum == current_checksum:
        return

    os.remove(pointer_file_path)
    return create_pointer_file(client, target, checksum=checksum)


def create_external_file(client: "LocalClient", src: Path, dst: Path):
    """Create a new external file."""
    try:
        pointer_file = create_pointer_file(client, target=src)
        relative = os.path.relpath(pointer_file, dst.parent)
        os.symlink(relative, dst)
    except OSError as e:
        raise errors.OperationError("Could not create symbolic link") from e
