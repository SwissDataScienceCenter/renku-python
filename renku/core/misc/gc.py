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
"""Project cleanup management."""

from renku.command.command_builder.command import inject
from renku.core.constant import CACHE, RENKU_HOME, RENKU_TMP
from renku.core.interface.client_dispatcher import IClientDispatcher


@inject.autoparams()
def remove_caches(client_dispatcher: IClientDispatcher):
    """Remove caches and temporary files.

    Args:
        client_dispatcher(IClientDispatcher):  Injected client dispatcher.

    """
    client = client_dispatcher.current_client

    cache_paths = [CACHE, RENKU_TMP]
    paths = [client.path / RENKU_HOME / p for p in cache_paths]

    client.repository.clean(paths=paths)
