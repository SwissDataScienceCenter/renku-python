# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
from typing import Any, Dict, List, Optional, Tuple

from renku.core.management.client import LocalClient


class Session:
    """Interactive session."""

    def __init__(self, id: str, status: str, url: str):
        self.id = id
        self.status = status
        self.url = url


class ISessionProvider(metaclass=ABCMeta):
    """Abstract class for a interactive session provider."""

    @abstractmethod
    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]) -> Optional[str]:
        """Builds the container image.

        Args:
            image_descriptor: Path to the container image descriptor file.
            image_name: Container image name.
            config: Path to the session provider specific configuration YAML.

        Returns:
            str: a unique id for the created interactive session.
        """
        pass

    @abstractmethod
    def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
        """Search for the given container image.

        Args:
            image_name: Container image name.
            config: Path to the session provider specific configuration YAML.

        Returns:
            bool: True if the given container images is available locally.
        """
        pass

    @abstractmethod
    def session_provider(self) -> Tuple["ISessionProvider", str]:
        """Supported session provider.

        Returns:
            a tuple of ``self`` and engine type name.
        """
        pass

    @abstractmethod
    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        Args:
            project_name: Renku project name.
            config: Path to the session provider specific configuration YAML.

        Returns:
            a list of sessions.
        """
        pass

    @abstractmethod
    def session_start(
        self,
        image_name: str,
        project_name: str,
        config: Optional[Dict[str, Any]],
        client: LocalClient,
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
    ) -> str:
        """Creates an interactive session.

        Args:
            image_name: Container image name to be used for the interactive session.
            project_name: The project identifier.
            config: Path to the session provider specific configuration YAML.
            client: Renku client.
            cpu_request: CPU request for the session.
            mem_request: Memory size request for the session.
            disk_request: Disk size request for the session.
            gpu_request: GPU device request for the session.

        Returns:
            str: a unique id for the created interactive session.
        """
        pass

    @abstractmethod
    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
        """Stops all or a given interactive session.

        Args:
            client: Renku client.
            session_name: The unique id of the interactive session.
            stop_all: Specifies whether or not to stop all the running interactive sessions.


        Returns:
            bool: True in case session(s) has been successfully stopped
        """
        pass

    @abstractmethod
    def session_url(self, session_name: str) -> Optional[str]:
        """Get the given session's URL.

        Args:
            session_name: The unique id of the interactive session.

        Returns:
            URL of the interactive session.
        """
        pass
