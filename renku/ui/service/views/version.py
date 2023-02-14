# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service version view."""

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.version import VersionCtrl
from renku.ui.service.views.api_versions import ALL_VERSIONS, MAXIMUM_VERSION, MINIMUM_VERSION, VersionedBlueprint
from renku.ui.service.views.error_handlers import handle_common_except

VERSION_BLUEPRINT_TAG = "version"
version_blueprint = VersionedBlueprint("version", __name__, url_prefix=SERVICE_PREFIX)


@version_blueprint.route("/apiversion", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
def version():
    """
    Version view.

    ---
    get:
      description: Show the service version.
      responses:
        200:
          description: The service version.
          content:
            application/json:
              schema: VersionResponseRPC
      tags:
        - version
    """
    return VersionCtrl().to_response(MINIMUM_VERSION, MAXIMUM_VERSION)
