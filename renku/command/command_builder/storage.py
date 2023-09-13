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
"""Command builder for storage api."""


from renku.command.command_builder.command import Command, check_finalized
from renku.core.interface.storage_service_gateway import IStorageService
from renku.domain_model.project_context import project_context
from renku.infrastructure.storage.storage_service import StorageService


class StorageApiCommand(Command):
    """Builder to get a storage api client."""

    PRE_ORDER = 4

    def __init__(self, builder: Command) -> None:
        self._builder = builder

    def _injection_pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Create a storage api provider."""

        if not project_context.has_context():
            raise ValueError("storage api builder needs a ProjectContext to be set.")

        context["constructor_bindings"][IStorageService] = lambda: StorageService()

    @check_finalized
    def build(self) -> Command:
        """Build the command."""
        self._builder.add_injection_pre_hook(self.PRE_ORDER, self._injection_pre_hook)

        return self._builder.build()
