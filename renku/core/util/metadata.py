# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Helpers functions for metadata management/parsing."""

import os
import re
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

from packaging.version import Version

from renku.core import errors
from renku.core.constant import RENKU_HOME, RENKU_TMP
from renku.core.util.os import is_subpath

if TYPE_CHECKING:
    from renku.domain_model.provenance.agent import Person


def construct_creators(
    creators: List[Union[dict, str]], ignore_email=False
) -> Tuple[List["Person"], List[Union[dict, str]]]:
    """Parse input and return a list of Person."""
    creators = creators or []

    if not isinstance(creators, Iterable) or isinstance(creators, str):
        raise errors.ParameterError("Invalid creators type")

    people = []
    no_email_warnings = []
    for creator in creators:
        person, no_email_warning = construct_creator(creator, ignore_email=ignore_email)

        if person:
            people.append(person)

        if no_email_warning:
            no_email_warnings.append(no_email_warning)

    return people, no_email_warnings


def construct_creator(creator: Union[dict, str], ignore_email) -> Tuple[Optional["Person"], Optional[Union[dict, str]]]:
    """Parse input and return an instance of Person."""
    from renku.domain_model.provenance.agent import Person

    if not creator:
        return None, None

    if isinstance(creator, str):
        person = Person.from_string(creator)
    elif isinstance(creator, dict):
        person = Person.from_dict(creator)
    else:
        raise errors.ParameterError("Invalid creator type")

    message = 'A valid format is "Name <email> [affiliation]"'

    if not person.name:  # pragma: no cover
        raise errors.ParameterError(f'Name is invalid: "{creator}".\n{message}')

    if not person.email:
        if not ignore_email:  # pragma: no cover
            raise errors.ParameterError(f'Email is invalid: "{creator}".\n{message}')
        else:
            no_email_warning = creator
    else:
        no_email_warning = None

    return person, no_email_warning


def is_external_file(path: Union[Path, str], client_path: Path):
    """Checks if a path is an external file."""
    from renku.core.constant import RENKU_HOME
    from renku.core.dataset.constant import POINTERS

    path = client_path / path
    if not path.is_symlink() or not is_subpath(path=path, base=client_path):
        return False

    pointer = os.readlink(path)
    return str(os.path.join(RENKU_HOME, POINTERS)) in pointer


def get_renku_version(client) -> Optional[str]:
    """Return project's Renku version from its Dockerfile."""
    return read_renku_version_from_dockerfile(client.docker_path)


def read_renku_version_from_dockerfile(path: Union[Path, str]) -> Optional[str]:
    """Read RENKU_VERSION from the content of path if a valid version is available."""
    path = Path(path)
    if not path.exists():
        return None

    docker_content = path.read_text()
    m = re.search(r"^\s*ARG RENKU_VERSION=(.+)$", docker_content, flags=re.MULTILINE)
    if not m:
        return None

    try:
        return str(Version(m.group(1)))
    except ValueError:
        return None


def make_project_temp_dir(client_path: Path) -> Path:
    """Create a temporary directory inside project's temp path."""
    base = client_path / RENKU_HOME / RENKU_TMP
    base.mkdir(parents=True, exist_ok=True)

    return Path(tempfile.mkdtemp(dir=base))
