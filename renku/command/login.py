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
"""Login/logout commands."""

from renku.command.command_builder import Command
from renku.core.login import credentials, login, logout


def login_command():
    """Return a command for logging in to the platform."""
    return Command().command(login)


def logout_command():
    """Return a command for logging out from the platform."""
    return Command().command(logout)


def credentials_command():
    """Return a command as git credential helper."""
    return Command().command(credentials)
