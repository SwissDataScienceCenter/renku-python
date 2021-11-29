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

from renku.core.models.git import GitURL
from renku.core.utils.os import delete_file
from renku.service.interfaces.git_api_provider import IGitAPIProvider


class GitlabAPIProvider(IGitAPIProvider):
    """Interface a Git API Provider."""

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
        gl = gitlab.Gitlab(git_data.instance_url, private_token=token)
        project = gl.projects.get(f"{git_data.owner}/{git_data.name}")

        result_paths = []

        for path in paths:
            full_path = target_folder / path

            full_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(full_path, "wb") as f:
                    project.files.raw(file_path=path, ref=ref, streamed=True, action=f.write)

                result_paths.append(full_path)
            except gitlab.GitlabGetError:
                delete_file(full_path)
                continue
