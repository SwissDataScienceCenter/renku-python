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

from abc import ABC
from pathlib import Path
from typing import List, Optional, Union


class IGitAPIProvider(ABC):
    """Interface a Git API Provider."""

    def __init__(self, token: str):
        """Initialize class."""
        raise NotImplementedError()

    def download_files_from_api(
        self,
        files: List[Union[Path, str]],
        folders: List[Union[Path, str]],
        target_folder: Union[Path, str],
        remote: str,
        branch: Optional[str] = None,
    ):
        """Download files through a remote Git API."""
        raise NotImplementedError()

    def get_project_id(self, gitlab_url: str, namespace: str, name: str) -> Optional[str]:
        """Get a gitlab project id from namespace/name."""
        raise NotImplementedError()
