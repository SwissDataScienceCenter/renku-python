# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
from pathlib import Path
from typing import List, Optional, Tuple

import pluggy

from renku.core.management.client import LocalClient
from renku.core.models.session import ISessionProvider

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def session_provider() -> Tuple[ISessionProvider, str]:
    """Plugin Hook for ``session`` sub-command.

    :returns: A tuple of the plugin itself and the name of the plugin.
    """
    pass


# FIXME: we might just want to return all the plugins results at once.
@hookspec(firstresult=True)
def session_list(self, config: Optional[Path], client: LocalClient) -> List[str]:
    """Lists all the sessions currently running by the given session provider.

    :returns: a list of sessions.
    """
    pass


@hookspec(firstresult=True)
def session_start(self, config: Path, image_name: Optional[str], client: LocalClient) -> str:
    """Creates an interactive session.

    :returns: a unique id for the created interactive sesssion.
    """
    pass


@hookspec
def session_stop(self, client: LocalClient, session_name: Optional[str], stop_all: bool):
    """Stops all or a given interactive session."""
    pass


@hookspec
def session_url(self, session_name: str) -> str:
    """Get the given sessions URL."""
    pass


def supported_session_providers() -> List[str]:
    """Returns the currently available interactive session provider types."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    supported_providers = pm.hook.session_provider()
    return [e[1] for e in supported_providers]
