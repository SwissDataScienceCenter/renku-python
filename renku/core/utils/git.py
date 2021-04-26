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
"""Git utility functions."""

import math
import pathlib
import urllib
from subprocess import SubprocessError, run

from renku.core import errors
from renku.core.models.git import GitURL

ARGUMENT_BATCH_SIZE = 100
CLI_GITLAB_ENDPOINT = "repos"


def run_command(command, *paths, separator=None, **kwargs):
    """Execute command by splitting `paths` to make sure that argument list will be within os limits.

    :param command: A list or tuple containing command and its arguments.
    :param paths: List of paths/long argument. This will be appended to `command` for each invocation.
    :param separator: Separator for `paths` if they need to be passed as string.
    :param kwargs: Extra arguments passed to `subprocess.run`.
    :returns: Result of last invocation.
    """
    result = None

    for batch in split_paths(*paths):
        if separator:
            batch = [separator.join(batch)]

        try:
            if not isinstance(batch, list):
                batch = list(batch)
            result = run(command + batch, **kwargs)

            if result.returncode != 0:
                break
        except KeyboardInterrupt:
            raise
        except SubprocessError as e:
            raise errors.GitError(f"Cannot run command {command} : {e}")

    return result


def add_to_git(git, *paths, **kwargs):
    """Split `paths` and add them to git to make sure that argument list will be within os limits."""
    for batch in split_paths(*paths):
        git.add(*batch, **kwargs)


def split_paths(*paths):
    """Return a generator with split list of paths."""
    batch_count = math.ceil(len(paths) / ARGUMENT_BATCH_SIZE)
    batch_count = max(batch_count, 1)

    for index in range(batch_count):
        yield paths[index * ARGUMENT_BATCH_SIZE : (index + 1) * ARGUMENT_BATCH_SIZE]


def get_oauth_url(url, gitlab_token):
    """Format URL with a username and password."""
    parsed_url = urllib.parse.urlparse(url)

    if not parsed_url.netloc:
        raise ValueError(f"Invalid http git url: {url}")

    netloc = f"oauth2:{gitlab_token}@{parsed_url.netloc}"
    return parsed_url._replace(netloc=netloc).geturl()


def have_same_remote(url1, url2):
    """Checks if two git urls point to the same remote repo ignoring protocol and credentials."""
    u1 = GitURL.parse(url1)
    u2 = GitURL.parse(url2)

    return u1.hostname == u2.hostname and u1.pathname == u2.pathname


def get_renku_repo_url(remote_url, deployment_hostname=None, access_token=None):
    """Return a repo url that can be authenticated by renku."""
    parsed_remote = GitURL.parse(remote_url)
    path = parsed_remote.pathname.strip("/")
    if path.startswith("gitlab/"):
        path = path.replace("gitlab/", "")
    path = pathlib.posixpath.join(CLI_GITLAB_ENDPOINT, path)

    credentials = f"renku:{access_token}@" if access_token else ""
    hostname = deployment_hostname or parsed_remote.hostname

    return urllib.parse.urljoin(f"https://{credentials}{hostname}", path)
