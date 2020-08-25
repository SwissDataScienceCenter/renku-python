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

from git import Repo
from marshmallow import EXCLUDE

from renku.core.utils.contexts import chdir
from renku.service.serializers.cache import ProjectCloneContext


class RemoteProject:
    """Parent controller for all controllers with remote support."""

    def __init__(self, user_data, request_data, branch="master"):
        """Construct remote controller."""
        self.ctx = ProjectCloneContext().load({**user_data, **request_data}, unknown=EXCLUDE)

        self.git_url = self.ctx["url_with_auth"]
        self.branch = branch

    @contextmanager
    def remote(self):
        """Retrieve project metadata."""
        with tempfile.TemporaryDirectory() as td, chdir(td):
            Repo.clone_from(self.git_url, td, branch=self.branch, depth=1)
            yield td
