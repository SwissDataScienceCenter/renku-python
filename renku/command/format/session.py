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

from renku.command.format.tabulate import tabulate


def tabular(sessions, *, columns=None):
    """Format workflows with a tabular output."""
    if not columns:
        columns = "id,status,url"

        if any(s.ssh_enabled for s in sessions):
            columns += ",ssh"

    return tabulate(collection=sessions, columns=columns, columns_mapping=SESSION_COLUMNS)


SESSION_FORMATS = {"tabular": tabular}
"""Valid formatting options."""

SESSION_COLUMNS = {
    "id": ("id", "id"),
    "status": ("status", "status"),
    "url": ("url", "url"),
    "ssh": ("ssh_enabled", "SSH enabled"),
}
