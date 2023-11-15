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
"""Renku service version view."""
from flask import Blueprint

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.versions_list import VersionsListCtrl
from renku.ui.service.views.error_handlers import handle_common_except

VERSIONS_LIST_BLUEPRINT_TAG = "versions"
versions_list_blueprint = Blueprint("versions", __name__, url_prefix=SERVICE_PREFIX)


@versions_list_blueprint.route("/versions", methods=["GET"], provide_automatic_options=False)
@handle_common_except
def versions_list():
    """
    Shows the list of all supported metadata versions.

    ---
    get:
      description: Metadata versions supported by all deployed core services.
      responses:
        200:
          description: The list of metadata versions.
          content:
            application/json:
              schema: VersionsListResponseRPC
      tags:
        - version
    """
    return VersionsListCtrl().to_response()
