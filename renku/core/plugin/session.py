# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
"""Plugin hooks for renku workflow customization."""
from typing import List, Tuple

import pluggy

from renku.domain_model.session import ISessionProvider

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def session_provider() -> Tuple[ISessionProvider, str]:  # type: ignore[empty-body]
    """Plugin Hook for ``session`` sub-command.

    Returns:
        A tuple of the plugin itself and the name of the plugin.
    """
    pass


def get_supported_session_providers() -> List[ISessionProvider]:
    """Returns the currently available interactive session providers."""
    from renku.core.plugin.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    return pm.hook.session_provider()
