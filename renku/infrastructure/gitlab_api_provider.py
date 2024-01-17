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
"""Git APi provider interface."""

import tarfile
import tempfile
from pathlib import Path
from typing import Generator, List, Optional, Union

import gitlab

from renku.core import errors
from renku.core.interface.git_api_provider import IGitAPIProvider
from renku.core.util.os import delete_dataset_file
from renku.domain_model.git import GitURL
from renku.ui.service.logger import service_log


class GitlabAPIProvider(IGitAPIProvider):
    """GitLab API provider abstraction layer.

    Args:
        paths: List of files to download.
        target_folder: Folder to use to download the files.
        remote: Remote repository URL.
        token: User bearer token.
        ref: optional reference to check out,
    Raises:
        errors.ProjectNotFound: If the remote URL is not accessible.
        errors.AuthenticationError: If the bearer token is invalid in any way.
    """

    def __init__(self, token: str):
        """Init gitlab provider."""
        self.token = token

    def download_files_from_api(
        self,
        files: List[Union[Path, str]],
        folders: List[Union[Path, str]],
        target_folder: Union[Path, str],
        remote: str,
        branch: Optional[str] = None,
    ):
        """Download files through a remote Git API.

        Args:
            files(List[Union[Path, str]]): Files to download.
            folders(List[Union[Path, str]]): Folders to download.
            target_folder(Union[Path, str]): Destination to save downloads to.
            remote(str): Git remote URL.
            branch(Optional[str]): Git reference (Default value = None).
        """
        if not branch:
            branch = "HEAD"

        target_folder = Path(target_folder)

        git_data = GitURL.parse(remote)

        if git_data.name is None:
            raise errors.InvalidGitURL("Couldn't parse repo name from git url")

        project = self._get_project(git_data.instance_url, git_data.owner, git_data.name)

        for file in files:
            full_path = target_folder / file

            full_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(full_path, "wb") as f:
                    project.files.raw(file_path=str(file), ref=branch, streamed=True, action=f.write)
            except gitlab.GitlabGetError as e:
                service_log.info("Gitlab get error", exc_info=e)
                delete_dataset_file(full_path)
                continue

        for folder in folders:
            with tempfile.NamedTemporaryFile() as f:
                project.repository_archive(path=str(folder), sha=branch, streamed=True, action=f.write, format="tar.gz")
                f.seek(0)
                with tarfile.open(fileobj=f) as archive:
                    archive.extractall(path=target_folder, members=tar_members_without_top_folder(archive, 1))

    def get_project_id(self, gitlab_url: str, namespace: str, name: str) -> Optional[str]:
        """Get a gitlab project id from namespace/name."""
        project = self._get_project(gitlab_url, namespace, name)
        if not project:
            return None
        return project.id

    def _get_project(self, gitlab_url: str, namespace: str, name: str):
        """Get a gitlab project."""
        try:
            gl = gitlab.Gitlab(gitlab_url, oauth_token=self.token)
            project = gl.projects.get(f"{namespace}/{name}")
        except gitlab.GitlabAuthenticationError:
            # NOTE: Invalid or expired tokens fail even on public projects. Let's give it a try without tokens
            try:
                gl = gitlab.Gitlab(gitlab_url)
                project = gl.projects.get(f"{namespace}/{name}")
            except gitlab.GitlabAuthenticationError as e:
                raise errors.AuthenticationError from e
            except gitlab.GitlabGetError as e:
                # NOTE: better to re-raise this as a core error since it's a common case
                service_log.warn(f"fast project clone didn't work: {e}", exc_info=e)
                if "project not found" in getattr(e, "error_message", "").lower():
                    raise errors.ProjectNotFound from e
                else:
                    raise
        except gitlab.GitlabGetError as e:
            # NOTE: better to re-raise this as a core error since it's a common case
            service_log.warn(f"fast project clone didn't work: {e}", exc_info=e)
            if "project not found" in getattr(e, "error_message", "").lower():
                raise errors.ProjectNotFound from e
            else:
                raise
        return project


def tar_members_without_top_folder(tar: tarfile.TarFile, strip: int) -> Generator[tarfile.TarInfo, None, None]:
    """Gets tar members, ignoring the top folder."""
    for member in tar.getmembers():
        member.path = member.path.split("/", strip)[-1]
        yield member
