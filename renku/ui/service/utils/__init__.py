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
"""Renku service utility functions."""
import os
import urllib
from time import sleep
from typing import Any, Dict, Optional, overload

import requests
from jwt import PyJWKClient

from renku.core.util.requests import get
from renku.ui.service.config import CACHE_PROJECTS_PATH, CACHE_UPLOADS_PATH, OIDC_URL
from renku.ui.service.errors import ProgramInternalError
from renku.ui.service.logger import service_log


def make_project_path(user, project):
    """Construct full path for cached project."""
    from renku.ui.service.cache.models.project import NO_BRANCH_FOLDER

    valid_user = user and "user_id" in user
    valid_project = project and "owner" in project and "name" in project and "project_id" in project

    if valid_user and valid_project:
        return (
            CACHE_PROJECTS_PATH
            / user["user_id"]
            / project["owner"]
            / project["slug"]
            / project.get("branch", NO_BRANCH_FOLDER)
        )


def make_file_path(user, cached_file):
    """Construct full path for cache file."""
    valid_user = user and "user_id" in user
    valid_file = cached_file and "file_name" in cached_file

    if valid_user and valid_file:
        return CACHE_UPLOADS_PATH / user["user_id"] / cached_file["relative_path"]


def valid_file(user, cached_file):
    """Ensure file system and cache state matches."""
    file_path = make_file_path(user, cached_file)

    if file_path.exists():
        cached_file["is_dir"] = file_path.is_dir()
        return cached_file


def new_repo_push(repo_path, source_url, source_name="origin", source_branch="master"):
    """Push a new repo to origin."""
    from renku.core.util.git import push_changes
    from renku.infrastructure.repository import Repository

    repository = Repository(repo_path)
    repository.remotes.add(source_name, source_url)
    branch = push_changes(repository, remote=source_name)
    return branch == source_branch


@overload
def normalize_git_url(git_url: None) -> None:
    ...


@overload
def normalize_git_url(git_url: str) -> str:
    ...


def normalize_git_url(git_url: Optional[str]) -> Optional[str]:
    """Remove ``.git`` postfix from a repository's url."""
    if git_url is None:
        return None

    git_url = git_url.rstrip("/")

    while git_url.lower().endswith(".git"):
        git_url = git_url[: -len(".git")]

    return git_url


def oidc_discovery() -> Dict[str, Any]:
    """Query the OIDC discovery endpoint from Keycloak with retries, parse the result with JSON and it."""
    retries = 0
    max_retries = 30
    sleep_seconds = 2
    renku_domain = os.environ.get("RENKU_DOMAIN")
    if not renku_domain:
        raise ProgramInternalError(
            error_message="Cannot perform OIDC discovery without the renku domain expected "
            "to be found in the RENKU_DOMAIN environment variable."
        )
    full_oidc_url = f"http://{renku_domain}{OIDC_URL}"
    while True:
        retries += 1
        try:
            res: requests.Response = get(full_oidc_url)
        except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
            if not retries < max_retries:
                service_log.error("Failed to get OIDC discovery data after all retries - the server cannot start.")
                raise e
            service_log.info(
                f"Failed to get OIDC discovery data from {full_oidc_url}, "
                f"sleeping for {sleep_seconds} seconds and retrying"
            )
            sleep(sleep_seconds)
        else:
            service_log.info(f"Successfully fetched OIDC discovery data from {full_oidc_url}")
            return res.json()


def jwk_client() -> PyJWKClient:
    """Return a JWK client for Keycloak that can be used to provide JWT keys for JWT signature validation."""
    oidc_data = oidc_discovery()
    jwks_uri = oidc_data.get("jwks_uri")
    if not jwks_uri:
        raise ProgramInternalError(error_message="Could not find jwks_uri in the OIDC discovery data")
    jwk = PyJWKClient(jwks_uri)
    return jwk
