#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku session fixtures."""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def dummy_session_provider():
    """Register as dummy session provider."""
    from pathlib import Path
    from uuid import uuid4

    from renku.core.plugin import hookimpl
    from renku.core.plugin import pluginmanager as plugin_manager
    from renku.domain_model.session import ISessionProvider, Session

    class _DummySessionProvider(ISessionProvider):
        sessions = list()

        @property
        def name(self):
            return "dummy"

        def is_remote_provider(self):
            return False

        def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]):
            pass

        def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
            return True

        @hookimpl
        def session_provider(self) -> ISessionProvider:
            return self

        def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
            return [Session(id=n, status="running", url="http://localhost/") for n in self.sessions]

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
            name = f"session-random-{uuid4().hex}-name"
            self.sessions.append(name)
            return name, ""

        def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
            if stop_all:
                self.sessions.clear()
                return True

            self.sessions.remove(session_name)
            return True

        def session_url(self, session_name: str) -> Optional[str]:
            return "http://localhost/"

        def pre_start_checks(self, **kwargs):
            pass

        def get_start_parameters(self):
            return []

        def get_open_parameters(self):
            return []

        def session_open(self, project_name: str, session_name: str, **kwargs) -> bool:
            browser.open(self.session_url(session_name))
            return True

    plugin = _DummySessionProvider()
    pm = plugin_manager.get_plugin_manager()
    pm.register(plugin)

    browser = MagicMock()

    yield browser

    pm.unregister(plugin)
