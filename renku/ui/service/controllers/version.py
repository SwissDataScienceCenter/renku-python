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
"""Renku service version controller."""
import os

from renku import __version__
from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.serializers.version import VersionResponseRPC
from renku.ui.service.views import result_response


class VersionCtrl(ServiceCtrl):
    """Version controller."""

    RESPONSE_SERIALIZER = VersionResponseRPC()

    def to_response(self, minimum_version, maximum_version):
        """Serialize to service version response."""
        return result_response(
            VersionCtrl.RESPONSE_SERIALIZER,
            {
                "latest_version": __version__,
                "supported_project_version": SUPPORTED_PROJECT_VERSION,
                "cli_version": os.environ.get("RENKU_PROJECT_DEFAULT_CLI_VERSION") or __version__,
                "minimum_api_version": minimum_version.name,
                "maximum_api_version": maximum_version.name,
            },
        )
