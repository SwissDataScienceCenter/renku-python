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
"""Interactive session engine."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple

from renku.core.management.client import LocalClient


class ISessionProvider(metaclass=ABCMeta):
    """Abstract class for a interactive session provider."""

    @abstractmethod
    def session_provider(self) -> Tuple["ISessionProvider", str]:
        """Supported session provider.

        :returns: a tuple of ``self`` and engine type name.
        """
        pass

    @abstractmethod
    def session_list(self, config: Optional[Path], client: LocalClient) -> List[str]:
        """Lists all the sessions currently running by the given session provider.

        :param config: Path to the session provider specific configuration YAML.
        :param client: Renku client.
        :returns: a list of sessions.
        """
        pass

    @abstractmethod
    def session_start(self, config: Optional[Path], image_name: Optional[str], client: LocalClient) -> str:
        """Creates an interactive session.

        :param config: Path to the session provider specific configuration YAML.
        :param image_name: Container image name to be used for the interactive session.
        :param client: Renku client.
        :returns: a unique id for the created interactive sesssion.
        """
        pass

    @abstractmethod
    def session_stop(self, client: LocalClient, session_name: Optional[str], stop_all: bool):
        """Stops all or a given interactive session.

        :param client: Renku client.
        :param session_name: The unique id of the interactive session.
        :param stop_all: Specifies whether or not to stop all the running interactive sessions.
        """
        pass

    @abstractmethod
    def session_url(self, session_name: str) -> str:
        """Get the given session's URL.

        :param session_name: The unique id of the interactive session.
        :returns: URL of the interactive session.
        """
        pass
