# -*- coding: utf-8 -*-
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
"""Renku common fixtures."""

from typing import Generator, List

import pytest

from renku.core.util.communication import CommunicationCallback


class MockCommunication(CommunicationCallback):
    """Communication listener that outputs to internal arrays."""

    def __init__(self):
        super().__init__()
        self._stdout: List[str] = []
        self._stderr: List[str] = []

    @property
    def stdout(self) -> str:
        """Entire ``stdout``."""
        return "\n".join(self._stdout)

    @property
    def stdout_lines(self) -> List[str]:
        """Lines of ``stdout``."""
        return self._stdout

    @property
    def stderr(self) -> str:
        """Entire ``stderr``."""
        return "\n".join(self._stderr)

    @property
    def stderr_lines(self) -> List[str]:
        """Lines of ``stderr``."""
        return self._stderr

    def echo(self, msg, end="\n"):
        """Write a message."""
        self._write_to(msg)

    def info(self, msg):
        """Write an info message."""
        self._write_to(msg)

    def warn(self, msg):
        """Write a warning message."""
        self._write_to(msg)

    def error(self, msg):
        """Write an error message."""
        self._write_to(msg, output=self._stderr)

    def confirm(self, msg, abort=False, warning=False, default=False):
        """Get confirmation for an action."""
        return False

    def _write_to(self, message: str, output=None):
        output = output or self._stdout
        with CommunicationCallback.lock:
            output.extend(line.strip() for line in message.split("\n"))


@pytest.fixture
def mock_communication() -> Generator[MockCommunication, None, None]:
    """A mock communication fixture."""
    from renku.core.util import communication

    mock_communication = MockCommunication()
    communication.subscribe(mock_communication)

    try:
        yield mock_communication
    finally:
        communication.unsubscribe(mock_communication)
