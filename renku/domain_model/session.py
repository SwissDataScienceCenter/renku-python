# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from renku.core.constant import ProviderPriority

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter


class Session:
    """Interactive session."""

    def __init__(self, id: str, status: str, url: str, ssh_enabled: bool = False):
        self.id = id
        self.status = status
        self.url = url
        self.ssh_enabled = ssh_enabled


class ISessionProvider(metaclass=ABCMeta):
    """Abstract class for a interactive session provider."""

    priority: ProviderPriority = ProviderPriority.NORMAL

    @property
    @abstractmethod
    def name(self) -> str:
        """Return session provider's name."""
        pass

    @abstractmethod
    def is_remote_provider(self) -> bool:
        """Return True for remote providers (i.e. not local Docker)."""
        raise NotImplementedError

    @abstractmethod
    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]):
        """Builds the container image.

        Args:
            image_descriptor: Path to the container image descriptor file.
            image_name: Container image name.
            config: Path to the session provider specific configuration YAML.
        """
        pass

    @abstractmethod
    def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
        """Search for the given container image.

        Args:
            image_name(str): Container image name.
            config: Path to the session provider specific configuration YAML.

        Returns:
            bool: True if the given container images is available locally.
        """
        pass

    @abstractmethod
    def session_provider(self) -> ISessionProvider:
        """Supported session provider.

        Returns:
            a reference to ``self``.
        """
        pass

    @abstractmethod
    def get_start_parameters(self) -> List[ProviderParameter]:
        """Returns parameters that can be set for session start."""
        pass

    @abstractmethod
    def get_open_parameters(self) -> List[ProviderParameter]:
        """Returns parameters that can be set for session open."""
        pass

    @abstractmethod
    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        Args:
            project_name(str): Renku project name.
            config(Dict[str, Any], optional): Path to the session provider specific configuration YAML.

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
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
        **kwargs,
    ) -> Tuple[str, str]:
        """Creates an interactive session.

        Args:
            image_name(str): Container image name to be used for the interactive session.
            project_name(str): The project identifier.
            config(Optional[Dict[str, Any]]): Path to the session provider specific configuration YAML.
            cpu_request(Optional[float]): CPU request for the session.
            mem_request(Optional[str]): Memory size request for the session.
            disk_request(Optional[str]): Disk size request for the session.
            gpu_request(Optional[str]): GPU device request for the session.

        Returns:
            Tuple[str, str]: Provider message and a possible warning message.
        """
        pass

    @abstractmethod
    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
        """Stops all or a given interactive session.

        Args:
            project_name(str): Project's name.
            session_name(str, optional): The unique id of the interactive session.
            stop_all(bool): Specifies whether or not to stop all the running interactive sessions.


        Returns:
            bool: True in case session(s) has been successfully stopped
        """
        pass

    @abstractmethod
    def session_open(self, project_name: str, session_name: str, **kwargs) -> bool:
        """Open a given interactive session.

        Args:
            project_name(str): Renku project name.
            session_name(str): The unique id of the interactive session.
        """
        pass

    @abstractmethod
    def session_url(self, session_name: str) -> Optional[str]:
        """Get the given session's URL.

        Args:
            session_name(str): The unique id of the interactive session.

        Returns:
            URL of the interactive session.
        """
        pass

    def pre_start_checks(self, **kwargs):
        """Perform any required checks on the state of the repository prior to starting a session.

        The expectation is that this method will abort the
        session start if the checks are not successful or will take corrective actions to
        make sure that the session launches successfully. By default this method does not do any checks.
        """
        return None

    def force_build_image(self, **kwargs) -> bool:
        """Whether we should force build the image directly or check for an existing image first."""
        return False
