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
"""Renku service parent remote controller."""

import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import requests
from git import Repo

from renku.core.utils.contexts import chdir


class RemoteProject:
    """Parent controller for all controllers with remote support."""

    def __init__(self, remote_url, branch="master"):
        """Construct remote controller."""
        self.remote_url = remote_url
        self.branch = branch

    @contextmanager
    def remote(self):
        """Retrieve project metadata."""
        path = self.project_metadata_path

        with tempfile.TemporaryDirectory() as td, chdir(td):
            Repo.init(".")

            renku_dir = Path(f"{td}/.renku")
            renku_dir.mkdir(exist_ok=True)

            metadata_file = renku_dir / "metadata.yml"
            metadata_file.write_text(requests.get(path.geturl()).text)

            yield td

    @property
    def project_metadata_path(self):
        """Construct project metadata remote path."""
        url = urlparse(f"{self.remote_url}/raw/{self.branch}/.renku/metadata.yml")

        if url.scheme not in ("http", "https"):
            url = url._replace(scheme="https")

        if not url.netloc:
            raise ValueError("netloc unknown")

        return url
