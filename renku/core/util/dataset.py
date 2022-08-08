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
"""Helper utilities for datasets."""

import urllib
from typing import Tuple


def check_url(url: str) -> Tuple[bool, bool]:
    """Check if a url is local/remote and if it contains a git repository."""
    # NOTE: Supported scheme before refactoring were: "", "file", "http", "https", "git+https", "git+ssh"
    u = urllib.parse.urlparse(url)

    is_remote = u.scheme not in ("", "file") or url.lower().startswith("git@")
    is_git = is_remote and (u.path.endswith(".git") or u.scheme in ("git+https", "git+ssh") or url.startswith("git@"))

    return is_remote, is_git
