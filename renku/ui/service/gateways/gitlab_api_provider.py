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
"""Git APi provider interface."""

from pathlib import Path
from typing import List, Optional, Union

import gitlab

from renku.core import errors
from renku.core.util.os import delete_dataset_file
from renku.domain_model.git import GitURL
from renku.ui.service.interfaces.git_api_provider import IGitAPIProvider


class GitlabAPIProvider(IGitAPIProvider):
    """GitLab API provider abstraction layer.

    Args:
        paths: List of files to download.
        target_folder: Folder to use to download the files.
        remote: Remote repository URL.
        token: User bearer token.
        ref: optional reference to checkout,
    Raises:
        errors.ProjectNotFound: If the remote URL is not accessible.
        errors.AuthenticationError: If the bearer token is invalid in any way.
    """

    def download_files_from_api(
        self,
        paths: List[Union[Path, str]],
        target_folder: Union[Path, str],
        remote: str,
        token: str,
        ref: Optional[str] = None,
    ):
        """Download files through a remote Git API."""
        if not ref:
            ref = "HEAD"

        target_folder = Path(target_folder)

        git_data = GitURL.parse(remote)
        try:
            gl = gitlab.Gitlab(git_data.instance_url, oauth_token=token)
            project = gl.projects.get(f"{git_data.owner}/{git_data.name}")
        except gitlab.GitlabAuthenticationError:
            # NOTE: Invalid or expired tokens fail even on public projects. Let's give it a try without tokens
            try:
                gl = gitlab.Gitlab(git_data.instance_url)
                project = gl.projects.get(f"{git_data.owner}/{git_data.name}")
            except gitlab.GitlabAuthenticationError as e:
                raise errors.AuthenticationError from e
            except gitlab.GitlabGetError as e:
                # NOTE: better to re-raise this as a core error since it's a common case
                if "project not found" in getattr(e, "error_message", "").lower():
                    raise errors.ProjectNotFound from e
                else:
                    raise

        result_paths = []

        for path in paths:
            full_path = target_folder / path

            full_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(full_path, "wb") as f:
                    project.files.raw(file_path=str(path), ref=ref, streamed=True, action=f.write)

                result_paths.append(full_path)
            except gitlab.GitlabGetError:
                delete_dataset_file(full_path)
                continue
