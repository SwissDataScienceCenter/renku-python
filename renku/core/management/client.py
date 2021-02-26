# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Renku repository manager."""

import attr

from renku.core.management.config import ConfigManagerMixin
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.repository import PathMixin, RepositoryApiMixin
from renku.core.management.storage import StorageApiMixin


@attr.s
class LocalClient(PathMixin, StorageApiMixin, RepositoryApiMixin, DatasetsApiMixin, ConfigManagerMixin):
    """A low-level client for communicating with a local Renku repository."""
