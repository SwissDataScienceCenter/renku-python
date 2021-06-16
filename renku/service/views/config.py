# -*- coding: utf-8 -*-
#
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
"""Renku service project config views."""
from flask import Blueprint, request

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.config_set import SetConfigCtrl
from renku.service.controllers.config_show import ShowConfigCtrl
from renku.service.views.decorators import (
    accepts_json,
    handle_common_except,
    optional_identity,
    requires_cache,
    requires_identity,
)

CONFIG_BLUEPRINT_TAG = "config"
config_blueprint = Blueprint("config", __name__, url_prefix=SERVICE_PREFIX)


@config_blueprint.route("/config.show", methods=["GET"], provide_automatic_options=False)
@handle_common_except
@requires_cache
@optional_identity
def show_config(user_data, cache):
    """
    Retrieve the renku config for a project.

    ---
    get:
      description: Retrieve the renku config for a project.
      parameters:
        - in: query
          schema: ConfigShowRequest
      responses:
        200:
          description: Config of a renku project.
          content:
            application/json:
              schema: ConfigShowResponseRPC
      tags:
        - config
    """
    return ShowConfigCtrl(cache, user_data, dict(request.args)).to_response()


@config_blueprint.route("/config.set", methods=["POST"], provide_automatic_options=False)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def set_config(user_data, cache):
    """
    Set the renku config for a project.

    ---
    post:
      description: Set the renku config for a project.
      requestBody:
        content:
          application/json:
            schema: ConfigSetRequest
      responses:
        200:
          description: User and default configuration options.
          content:
            application/json:
              schema: ConfigSetResponseRPC
      tags:
        - config
    """
    return SetConfigCtrl(cache, user_data, dict(request.json)).to_response()
