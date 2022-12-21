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

import fnmatch
import os
import re
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

from packaging.version import Version

from renku.core import errors
from renku.core.config import get_value, set_value
from renku.core.constant import RENKU_HOME, RENKU_PROTECTED_PATHS, RENKU_TMP
from renku.core.util import communication
from renku.core.util.os import is_subpath

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderCredentials
    from renku.domain_model.provenance.agent import Person


def construct_creators(creators: List[Union[dict, str]], ignore_email=False) -> Tuple[List["Person"], List[str]]:
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


def construct_creator(creator: Union[dict, str], ignore_email) -> Tuple[Optional["Person"], Optional[str]]:
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
            no_email_warning = str(creator)
    else:
        no_email_warning = None

    return person, no_email_warning


def is_external_file(path: Union[Path, str], project_path: Path):
    """Checks if a path is an external file."""
    from renku.core.constant import POINTERS, RENKU_HOME

    path = project_path / path
    if not path.is_symlink() or not is_subpath(path=path, base=project_path):
        return False

    pointer = os.readlink(path)
    return str(os.path.join(RENKU_HOME, POINTERS)) in pointer


def read_renku_version_from_dockerfile(path: Optional[Union[Path, str]] = None) -> Optional[str]:
    """Read RENKU_VERSION from the content of path if a valid version is available."""
    from renku.domain_model.project_context import project_context

    path = Path(path) if path else project_context.docker_path
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


def make_project_temp_dir(project_path: Path) -> Path:
    """Create a temporary directory inside project's temp path."""
    base = project_path / RENKU_HOME / RENKU_TMP
    base.mkdir(parents=True, exist_ok=True)

    return Path(tempfile.mkdtemp(dir=base))


def store_credentials(section: str, key: str, value: str) -> None:
    """Write provider's credentials."""
    section, key = _get_section_and_key(section=section, key=key)
    set_value(section=section, key=key, value=value, global_only=True)


def read_credentials(section: str, key: str) -> Optional[str]:
    """Read provider's credentials."""
    section, key = _get_section_and_key(section=section, key=key)
    return get_value(section=section, key=key)


def get_canonical_key(key: str) -> str:
    """Make a consistent configuration key."""
    return key.replace(" ", "-").lower()


def _get_section_and_key(section: str, key: str) -> Tuple[str, str]:
    section = "http" if section.lower() == "renku" else section

    return section, get_canonical_key(key)


def prompt_for_credentials(provider_credentials: "ProviderCredentials") -> None:
    """Prompt for provider credentials if needed and update and store them."""
    if not provider_credentials:
        return

    provider_credentials.read()

    prompt_to_store = False
    for key in provider_credentials.get_credentials_names_with_no_value():
        prompt_to_store = True
        value = communication.prompt(f"Enter a value for '{key}'", type=str, default="")

        provider_credentials[key] = value

    if prompt_to_store:
        if communication.confirm("Store credentials?", default=True):
            provider_credentials.store()


def is_protected_path(path: Path) -> bool:
    """Checks if a path is a protected path."""
    from renku.domain_model.project_context import project_context

    try:
        path_in_repo = str(path.relative_to(project_context.path))
    except ValueError:
        return False

    for protected_path in RENKU_PROTECTED_PATHS:
        if fnmatch.fnmatch(path_in_repo, protected_path):
            return True

    return False
