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
"""Utility for working with HTTP session.

This module provides some wrapper functions around ``requests`` library. It sets a timeout and converts exception types
whenever needed. Use this module instead of ``requests``.
"""

import os
import tempfile
import urllib
from contextlib import contextmanager
from pathlib import Path
from typing import Union

import patoolib
import requests
from requests.adapters import HTTPAdapter, TimeoutSauce  # type: ignore
from urllib3.util.retry import Retry

from renku.core import errors

_RENKU_REQUESTS_TIMEOUT_SECONDS = float(os.getenv("RENKU_REQUESTS_TIMEOUT_SECONDS", 1200))


class _CustomTimeout(TimeoutSauce):
    """CustomTimeout for all HTTP requests."""

    def __init__(self, *args, **kwargs):
        """Construct CustomTimeout."""
        if kwargs["connect"] is None:
            kwargs["connect"] = _RENKU_REQUESTS_TIMEOUT_SECONDS

        if kwargs["read"] is None:
            kwargs["read"] = _RENKU_REQUESTS_TIMEOUT_SECONDS

        super().__init__(*args, **kwargs)


requests.adapters.TimeoutSauce = _CustomTimeout  # type: ignore


def delete(url, headers=None):
    """Send a DELETE request."""
    return _request("delete", url=url, headers=headers)


def get(url, headers=None, params=None):
    """Send a GET request."""
    return _request("get", url=url, headers=headers, params=params)


def head(url, *, allow_redirects=False, headers=None):
    """Send a HEAD request."""
    return _request("head", url=url, allow_redirects=allow_redirects, headers=headers)


def post(url, *, data=None, files=None, headers=None, json=None, params=None):
    """Send a POST request."""
    return _request("post", url=url, data=data, files=files, headers=headers, json=json, params=params)


def put(url, *, data=None, files=None, headers=None, params=None):
    """Send a PUT request."""
    return _request("put", url=url, data=data, files=files, headers=headers, params=params)


def _request(verb: str, url: str, *, allow_redirects=True, data=None, files=None, headers=None, json=None, params=None):
    try:
        with _retry() as session:
            return getattr(session, verb)(
                url=url,
                allow_redirects=allow_redirects,
                data=data,
                files=files,
                headers=headers,
                json=json,
                params=params,
            )
    except (ConnectionError, requests.RequestException, urllib.error.HTTPError) as e:
        raise errors.RequestError(f"{verb.upper()} request failed for {url}") from e


def get_redirect_url(url) -> str:
    """Return redirect URL if any; otherwise, return the original URL."""
    try:
        return head(url, allow_redirects=True).url
    except errors.RequestError:
        # NOTE: HEAD request failed, try with original url
        return url


def check_response(response):
    """Check for expected response status code."""
    if response.status_code in [200, 201, 202]:
        return
    elif response.status_code == 401:
        raise errors.AuthenticationError("Access unauthorized - update access token.")
    else:
        content = response.content.decode("utf-8") if response.content else ""
        message = f"Request failed with code {response.status_code}: {content}"
        raise errors.RequestError(message)


def download_file(base_directory: Union[Path, str], url: str, filename, extract, chunk_size=16384):
    """Download a URL to a given location."""
    from renku.core.util import communication

    def extract_dataset(filepath):
        """Extract downloaded file."""
        try:
            tmp = tempfile.mkdtemp()
            patoolib.extract_archive(str(filepath), outdir=tmp, verbosity=-1)
        except patoolib.util.PatoolError:
            return filepath.parent, [filepath]
        else:
            filepath.unlink()
            return Path(tmp), [p for p in Path(tmp).rglob("*")]

    tmp_root = Path(base_directory)
    tmp_root.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.mkdtemp(dir=tmp_root)

    try:
        with requests.get(url, stream=True, allow_redirects=True) as response:
            response.raise_for_status()

            if not filename:
                filename = get_filename_from_headers(response)

            if not filename:
                u = urllib.parse.urlparse(url)
                filename = Path(u.path).name
                if not filename:
                    raise errors.ParameterError(f"URL Cannot find a file to download from {url}")

            download_to = Path(tmp) / filename
            with open(str(download_to), "wb") as file_:
                total_size = int(response.headers.get("content-length", 0))

                communication.start_progress(name=filename, total=total_size)
                try:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:  # ignore keep-alive chunks
                            file_.write(chunk)
                            communication.update_progress(name=filename, amount=len(chunk))
                finally:
                    communication.finalize_progress(name=filename)

    except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:  # pragma nocover
        raise errors.RequestError(f"Cannot download from {url}") from e

    if extract:
        return extract_dataset(download_to)

    return download_to.parent, [download_to]


def get_filename_from_headers(response):
    """Extract filename from content-disposition headers if available."""
    content_disposition = response.headers.get("content-disposition", None)

    if not content_disposition:
        return None

    entries = content_disposition.split(";")
    name_entry = next((e.strip() for e in entries if e.strip().lower().startswith("filename*=")), None)

    if name_entry:
        name = name_entry.split("=", 1)[1].strip()
        encoding, _, name = name.split("'")
        return urllib.parse.unquote(name, encoding, errors="strict")

    name_entry = next((e.strip() for e in entries if e.strip().lower().startswith("filename=")), None)

    if not name_entry:
        return None

    filename = name_entry.split("=", 1)[1].strip()

    if filename.startswith('"'):
        filename = filename[1:-1]
    return filename


@contextmanager
def _retry(total_requests=5, backoff_factor=0.2, statuses=(500, 502, 503, 504, 429)):
    """Default HTTP session for requests."""
    _session = requests.Session()

    retries = Retry(total=total_requests, backoff_factor=backoff_factor, status_forcelist=list(statuses))

    _session.mount("http://", HTTPAdapter(max_retries=retries))
    _session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        yield _session
    except requests.RequestException as e:
        raise errors.RequestError("renku operation failed due to network connection failure") from e
