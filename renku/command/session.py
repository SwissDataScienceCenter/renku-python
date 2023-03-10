#
# Copyright 2018-2023- Swiss Data Science Center (SDSC)
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
"""Renku session commands."""


from renku.command.command_builder.command import Command
from renku.core.session.session import session_list, session_open, session_start, session_stop, ssh_setup


def session_list_command():
    """List all the running interactive sessions."""
    return Command().command(session_list)


def session_start_command():
    """Start an interactive session."""
    return Command().command(session_start).with_database().require_migration()


def session_stop_command():
    """Stop a running an interactive session."""
    return Command().command(session_stop)


def session_open_command():
    """Open a running interactive session."""
    return Command().command(session_open)


def ssh_setup_command():
    """Setup SSH keys for SSH connections to sessions."""
    return Command().command(ssh_setup)
