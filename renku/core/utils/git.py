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
import subprocess
import tempfile
import urllib
from pathlib import Path
from subprocess import SubprocessError, run
from typing import Optional, Union

from git import Commit, Git, GitCommandError, Repo

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


def get_object_hash(repo: Repo, path: Union[Path, str], revision: str = None) -> Optional[str]:
    """Return git hash of an object in a Repo or its submodule.

    NOTE: path must be relative to the repo's root regardless if this function is called from a subdirectory or not.
    """

    def get_object_hash_from_submodules() -> Optional[str]:
        for submodule in repo.submodules:
            try:
                path_in_submodule = Path(path).relative_to(submodule.path)
            except ValueError:
                continue
            else:
                try:
                    return Git(submodule.abspath).rev_parse(f"HEAD:{str(path_in_submodule)}")
                except GitCommandError:
                    pass

    revision = revision or "HEAD"

    try:
        return repo.git.rev_parse(f"{revision}:{str(path)}")
    except GitCommandError:
        # NOTE: The file can be in a submodule or it was not there when the command ran but was there when workflows
        # were migrated (this can happen only for Usage); the project might be broken too.
        return get_object_hash_from_submodules()


def get_content(repo: Repo, path: Union[Path, str], checksum: str) -> str:
    """Get content of an object from git using its checksum and return a path with that content."""

    def get_content_from_submodules(file):
        for submodule in repo.submodules:
            try:
                path_in_submodule = Path(path).relative_to(submodule.path)
            except ValueError:
                continue
            else:
                try:
                    subprocess.run(
                        ["git", "cat-file", "--filters", "--path", str(path_in_submodule), checksum],
                        check=True,
                        stdout=file,
                        cwd=repo.working_dir,
                    )
                except subprocess.CalledProcessError:
                    pass
                else:
                    return True

    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as temp_file:
        try:
            subprocess.run(
                ["git", "cat-file", "--filters", "--path", str(path), checksum],
                check=True,
                stdout=temp_file,
                cwd=repo.working_dir,
            )
        except subprocess.CalledProcessError:
            if not get_content_from_submodules(temp_file):
                raise errors.ExportError(f"File not found in repo: {checksum}:{path}")

    return temp_file.name


def find_previous_commit(
    repo: Repo, path: Union[Path, str], revision: str = None, return_first=False, full_history=False
) -> Optional[Commit]:
    """Return a previous commit for a given path starting from ``revision``.

    :param path: relative path to the file
    :param revision: revision to start from, defaults to ``HEAD``
    :param return_first: show the first commit in the history
    :param full_history: search full history
    """

    def get_previous_commit_from_submodules() -> Optional[Commit]:
        for submodule in repo.submodules:
            try:
                path_in_submodule = Path(path).relative_to(submodule.path)
            except ValueError:
                continue
            else:
                try:
                    return find_previous_commit(
                        repo=Repo(submodule.abspath),
                        path=path_in_submodule,
                        revision=revision,
                        return_first=return_first,
                        full_history=full_history,
                    )
                except KeyError:
                    pass

    path = str(path)
    revision = revision or "HEAD"
    kwargs = {}

    if full_history:
        kwargs["full_history"] = True
    if not return_first:
        kwargs["max_count"] = 1

    commits = list(repo.iter_commits(revision, paths=path, **kwargs))

    if not commits:
        commit = get_previous_commit_from_submodules()
        return commit

    return commits[-1 if return_first else 0]


def get_path(url: str):
    """Return path part of a url."""
    return urllib.parse.urlparse(url).path
