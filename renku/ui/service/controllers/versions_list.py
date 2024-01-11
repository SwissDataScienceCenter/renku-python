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
import json

from renku.ui.service import config
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.serializers.versions_list import VersionsListResponseRPC
from renku.ui.service.views import result_response


class VersionsListCtrl(ServiceCtrl):
    """Versions list controller."""

    RESPONSE_SERIALIZER = VersionsListResponseRPC()

    def to_response(self):
        """Serialize to service version response."""

        with open(config.METADATA_VERSIONS_LIST, "r") as f:
            return result_response(self.RESPONSE_SERIALIZER, json.load(f))
