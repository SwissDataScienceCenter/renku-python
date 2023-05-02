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
"""Serializers for sessions."""

from typing import List, Optional

from renku.command.format.tabulate import tabulate
from renku.domain_model.session import Session


def tabular(sessions: List[Session], *, columns: Optional[str] = None):
    """Format sessions with a tabular output."""
    if not columns:
        columns = "id,start_time,status,provider,url"

        if any(s.ssh_enabled for s in sessions):
            columns += ",ssh"

    return tabulate(collection=sessions, columns=columns, columns_mapping=SESSION_COLUMNS)


def log(sessions: List[Session], *, columns: Optional[str] = None):
    """Format sessions in a log like output."""
    from renku.ui.cli.utils.terminal import style_header, style_key

    output = []

    for session in sessions:
        output.append(style_header(f"Session {session.id} ({session.status}, {session.provider})"))
        output.append(style_key("Started: ") + session.start_time.isoformat())
        output.append(style_key("Url: ") + session.url)
        output.append(style_key("Commit: ") + session.commit)
        output.append(style_key("Branch: ") + session.branch)
        output.append(style_key("SSH enabled: ") + ("yes" if session.ssh_enabled else "no"))
        output.append("")
    return "\n".join(output)


SESSION_FORMATS = {"tabular": tabular, "log": log}
"""Valid formatting options."""

SESSION_COLUMNS = {
    "id": ("id", "id"),
    "status": ("status", "status"),
    "url": ("url", "url"),
    "ssh": ("ssh_enabled", "SSH enabled"),
    "start_time": ("start_time", "start_time"),
    "commit": ("commit", "commit"),
    "branch": ("branch", "branch"),
    "provider": ("provider", "provider"),
}
