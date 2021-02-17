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
from flask_apispec import marshal_with, use_kwargs

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.config_set import SetConfigCtrl
from renku.service.controllers.config_show import ShowConfigCtrl
from renku.service.serializers.config import (
    ConfigSetRequest,
    ConfigSetResponseRPC,
    ConfigShowRequest,
    ConfigShowResponseRPC,
)
from renku.service.views.decorators import (
    accepts_json,
    handle_common_except,
    header_doc,
    optional_identity,
    requires_cache,
    requires_identity,
)

CONFIG_BLUEPRINT_TAG = "config"
config_blueprint = Blueprint("config", __name__, url_prefix=SERVICE_PREFIX)


@use_kwargs(ConfigShowRequest, location="query")
@marshal_with(ConfigShowResponseRPC)
@header_doc(description="Show renku config for a project.", tags=(CONFIG_BLUEPRINT_TAG,))
@config_blueprint.route(
    "/config.show", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@optional_identity
def show_config(user_data, cache):
    """Show renku config for a project."""
    return ShowConfigCtrl(cache, user_data, dict(request.args)).to_response()


@use_kwargs(ConfigSetRequest)
@marshal_with(ConfigSetResponseRPC)
@header_doc(description="Set renku config for a project.", tags=(CONFIG_BLUEPRINT_TAG,))
@config_blueprint.route(
    "/config.set", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def set_config(user_data, cache):
    """Set renku config for a project."""
    return SetConfigCtrl(cache, user_data, dict(request.json)).to_response()
