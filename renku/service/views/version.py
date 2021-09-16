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
from flask import Blueprint

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.version import VersionCtrl
from renku.service.views.decorators import handle_validation_except

VERSION_BLUEPRINT_TAG = "version"
version_blueprint = Blueprint("version", __name__, url_prefix=SERVICE_PREFIX)


@version_blueprint.route("/version", methods=["GET"], provide_automatic_options=False)
@handle_validation_except
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
    return VersionCtrl().to_response()
