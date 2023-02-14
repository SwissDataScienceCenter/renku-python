# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Helper utilities for handling URLs."""

import os
import re
import unicodedata
import urllib
from typing import List, Optional
from urllib.parse import ParseResult, urlparse

from renku.core import errors
from renku.core.config import get_value
from renku.core.util.git import get_remote, parse_git_url
from renku.core.util.os import is_subpath
from renku.domain_model.project_context import project_context


def url_to_string(url):
    """Convert url from ``list`` or ``ParseResult`` to string."""
    if isinstance(url, list):
        return ParseResult(scheme=url[0], netloc=url[1], path=url[2], params="", query="", fragment="").geturl()

    if isinstance(url, ParseResult):
        return url.geturl()

    if isinstance(url, str):
        return url

    raise ValueError("url value not recognized")


def remove_credentials(url):
    """Remove username and password from a URL."""
    if url is None:
        return ""
    parsed = urllib.parse.urlparse(url)
    return parsed._replace(netloc=parsed.hostname).geturl()


def get_host(use_project_context: bool = True):
    """Return the hostname for the resource URIs.

    Default is localhost. If RENKU_DOMAIN is set, it overrides the host from remote.
    """
    host = "localhost"

    if use_project_context:
        host = project_context.remote.host or host

    return os.environ.get("RENKU_DOMAIN") or host


def get_path(url: str) -> str:
    """Return path part of a url."""
    return urllib.parse.urlparse(url).path


def get_scheme(uri: str) -> str:
    """Return scheme of a URI."""
    return urllib.parse.urlparse(uri).scheme.lower()


def parse_authentication_endpoint(endpoint: Optional[str] = None, use_remote: bool = False):
    """Return a parsed url.

    If an endpoint is provided then use it, otherwise, look for a configured endpoint. If no configured endpoint exists
    then try to use project's remote url.
    """
    if not endpoint:
        endpoint = get_value(section="renku", key="endpoint")
        if not endpoint:
            if not use_remote:
                return
            remote = get_remote(project_context.repository)
            if not remote or not remote.url:
                return
            endpoint = f"https://{parse_git_url(remote.url).hostname}/"

    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"

    parsed_endpoint = urllib.parse.urlparse(endpoint)
    if not parsed_endpoint.netloc:
        raise errors.ParameterError(f"Invalid endpoint: `{endpoint}`.")

    return parsed_endpoint._replace(scheme="https", path="/", params="", query="", fragment="")


def get_slug(name: str, invalid_chars: Optional[List[str]] = None, lowercase: bool = True) -> str:
    """Create a slug from name."""
    invalid_chars = invalid_chars or []
    lower_case = name.lower() if lowercase else name
    no_space = re.sub(r"\s+", "_", lower_case)
    normalized = unicodedata.normalize("NFKD", no_space).encode("ascii", "ignore").decode("utf-8")

    valid_chars_pattern = [r"\w", ".", "_", "-"]
    if len(invalid_chars) > 0:
        valid_chars_pattern = [ch for ch in valid_chars_pattern if ch not in invalid_chars]

    no_invalid_characters = re.sub(f'[^{"".join(valid_chars_pattern)}]', "_", normalized)
    no_duplicates = re.sub(r"([._-])[._-]+", r"\1", no_invalid_characters)
    valid_start = re.sub(r"^[._-]", "", no_duplicates)
    valid_end = re.sub(r"[._-]$", "", valid_start)
    no_dot_lock_at_end = re.sub(r"\.lock$", "_lock", valid_end)
    return no_dot_lock_at_end


def is_uri_subfolder(uri: str, subfolder_uri: str) -> bool:
    """Check if one uri is a 'subfolder' of another."""
    parsed_uri = urlparse(uri)
    parsed_subfolder_uri = urlparse(subfolder_uri)
    parsed_uri_path = parsed_uri.path
    parsed_subfolder_uri_path = parsed_subfolder_uri.path
    if parsed_uri_path in ["", "."]:
        # NOTE: s3://test has a path that equals "" and Path("") gets interpreted as Path(".")
        # this becomes a problem then when s3://test/1 has an "absolute-like" path of Path("/1")
        # and Path(".") is not considered a subpath of Path("/1") but from the uris we see that this
        # is indeed a subpath
        parsed_uri_path = "/"
    if parsed_subfolder_uri_path in ["", "."]:
        parsed_subfolder_uri_path = "/"
    if parsed_uri.scheme != parsed_subfolder_uri.scheme:
        # INFO: catch s3://test vs http://test
        return False
    if parsed_uri.netloc != parsed_subfolder_uri.netloc:
        # INFO: catch s3://test1 vs s3://test2
        return False
    return is_subpath(parsed_subfolder_uri_path, parsed_uri_path)
