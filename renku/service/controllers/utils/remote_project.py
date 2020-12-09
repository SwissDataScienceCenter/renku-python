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
"""Utilities for renku service controllers."""

import tempfile
from contextlib import contextmanager
from urllib.parse import urlparse

from git import GitCommandError, Repo
from marshmallow import EXCLUDE

from renku.core import errors
from renku.core.utils.contexts import click_context
from renku.service.serializers.cache import ProjectCloneContext

ANONYMOUS_SESSION = "anonymous"


class RemoteProject:
    """Parent controller for all controllers with remote support."""

    def __init__(self, user_data, request_data):
        """Construct remote controller."""
        if not user_data:
            user_data = {
                "owner": f"{ANONYMOUS_SESSION} session",
                "name": f"{ANONYMOUS_SESSION}",
                "token": f"{ANONYMOUS_SESSION}",
            }

        self.ctx = ProjectCloneContext().load({**user_data, **request_data}, unknown=EXCLUDE)

        self.git_url = self.ctx["url_with_auth"]
        self.branch = self.ctx["ref"]

    @property
    def remote_url(self):
        """Construct project metadata remote path."""
        url = urlparse(self.git_url)

        if url.scheme not in ("http", "https"):
            return url

        if not url.netloc:
            raise ValueError("netloc unknown")

        return url

    @contextmanager
    def remote(self):
        """Retrieve project metadata."""
        with tempfile.TemporaryDirectory() as td:
            try:
                Repo.clone_from(self.remote_url.geturl(), td, branch=self.branch, depth=1)
            except GitCommandError as e:
                msg = str(e)
                if "is not a commit and a branch" in msg and "cannot be created from it" in msg:
                    raise errors.UninitializedProject(td) from e  # NOTE: Project has no commits to check out

                raise

            with click_context(td, "remote_project"):
                yield td
