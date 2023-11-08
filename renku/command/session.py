#  Copyright Swiss Data Science Center (SDSC). A partnership between
#  École Polytechnique Fédérale de Lausanne (EPFL) and
#  Eidgenössische Technische Hochschule Zürich (ETHZ).
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
"""Renku session commands."""

from renku.command.command_builder.command import Command
from renku.core.session.session import (
    search_hibernating_session_providers,
    search_session_providers,
    search_sessions,
    session_list,
    session_open,
    session_pause,
    session_resume,
    session_start,
    session_stop,
    ssh_setup,
)


def search_sessions_command():
    """Get all the session names that match a pattern."""
    return Command().command(search_sessions).require_migration().with_database(write=False)


def search_session_providers_command():
    """Get all the session provider names that match a pattern."""
    return Command().command(search_session_providers).require_migration().with_database(write=False)


def search_hibernating_session_providers_command():
    """Get all the session provider names that support hibernation and match a pattern."""
    return Command().command(search_hibernating_session_providers).require_migration().with_database(write=False)


def session_list_command():
    """List all the running interactive sessions."""
    return Command().command(session_list).with_database(write=False)


def session_start_command():
    """Start an interactive session."""
    return Command().command(session_start).with_database().require_migration().with_gitlab_api().with_storage_api()


def session_stop_command():
    """Stop a running an interactive session."""
    return Command().command(session_stop).with_database(write=False)


def session_open_command():
    """Open a running interactive session."""
    return Command().command(session_open).with_database(write=False)


def ssh_setup_command():
    """Setup SSH keys for SSH connections to sessions."""
    return Command().command(ssh_setup)


def session_pause_command():
    """Pause a running interactive session."""
    return Command().command(session_pause).with_database(write=False)


def session_resume_command():
    """Resume a paused session."""
    return Command().command(session_resume).with_database(write=False)
