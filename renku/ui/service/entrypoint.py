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
"""Renku service entry point."""
import logging
import os
import traceback
import uuid

import sentry_sdk
from flask import Flask, Response, jsonify, request, url_for
from jwt import InvalidTokenError
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.rq import RqIntegration

from renku.ui.service.cache import cache
from renku.ui.service.config import CACHE_DIR, MAX_CONTENT_LENGTH, SENTRY_ENABLED, SENTRY_SAMPLERATE, SERVICE_PREFIX
from renku.ui.service.errors import (
    ProgramHttpMethodError,
    ProgramHttpMissingError,
    ProgramHttpRequestError,
    ProgramHttpServerError,
    ProgramHttpTimeoutError,
    ServiceError,
)
from renku.ui.service.logger import service_log
from renku.ui.service.serializers.headers import JWT_TOKEN_SECRET
from renku.ui.service.utils.json_encoder import SvcJSONEncoder
from renku.ui.service.views import error_response
from renku.ui.service.views.apispec import apispec_blueprint
from renku.ui.service.views.cache import cache_blueprint
from renku.ui.service.views.config import config_blueprint
from renku.ui.service.views.datasets import dataset_blueprint
from renku.ui.service.views.graph import graph_blueprint
from renku.ui.service.views.jobs import jobs_blueprint
from renku.ui.service.views.project import project_blueprint
from renku.ui.service.views.templates import templates_blueprint
from renku.ui.service.views.version import version_blueprint
from renku.ui.service.views.workflow_plans import workflow_plans_blueprint

logging.basicConfig(level=os.getenv("SERVICE_LOG_LEVEL", "WARNING"))

if SENTRY_ENABLED:
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        environment=os.getenv("SENTRY_ENV"),
        traces_sample_rate=float(SENTRY_SAMPLERATE),
        integrations=[FlaskIntegration(), RqIntegration(), RedisIntegration()],
    )


def create_app(custom_exceptions=True):
    """Creates a Flask app with a necessary configuration."""
    app = Flask(__name__)
    app.secret_key = os.getenv("RENKU_SVC_SERVICE_KEY", uuid.uuid4().hex)
    app.json_encoder = SvcJSONEncoder
    app.config["UPLOAD_FOLDER"] = CACHE_DIR

    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

    app.config["cache"] = cache

    build_routes(app)

    @app.route(SERVICE_PREFIX)
    def root():
        """Root shows basic service information."""
        import renku

        return jsonify({"service_version": renku.__version__, "spec_url": url_for("apispec.openapi")})

    @app.route("/health")
    def health():
        """Service health check."""
        import renku

        return "renku repository service version {}\n".format(renku.__version__)

    if custom_exceptions:
        register_exceptions(app)

    return app


def register_exceptions(app):
    """Register the exceptions handler."""

    @app.errorhandler(Exception)
    def exceptions(e):
        """Exception handler that manages Flask/Werkzeug exceptions.

        For the other exception handlers check ``service/decorators.py``
        """
        # NOTE: add log entry
        str(getattr(e, "code", "unavailable"))
        log_error_code = str(getattr(e, "code", "unavailable"))
        service_log.error(
            f"{request.remote_addr} {request.method} {request.scheme} {request.full_path}\n"
            f"Error code: {log_error_code}\n"
            f"Stack trace: {traceback.format_exc()}"
        )

        # NOTE: craft user messages
        if hasattr(e, "code"):
            code = int(e.code)

            # NOTE: return an http error for methods with no body allowed. This prevents undesired exceptions.
            NO_PAYLOAD_METHODS = "HEAD"
            if request.method in NO_PAYLOAD_METHODS:
                return Response(status=code)

            error: ServiceError
            if code == 400:
                error = ProgramHttpRequestError(e)
            elif code == 404:
                error = ProgramHttpMissingError(e)
            elif code == 405:
                error = ProgramHttpMethodError(e)
            elif code == 408:
                error = ProgramHttpTimeoutError(e)
            else:
                error = ProgramHttpServerError(e, code)

            return error_response(error)

        # NOTE: Werkzeug exceptions should be covered above, the following line is for
        #   unexpected HTTP server errors.
        return error_response(e)


def build_routes(app):
    """Register routes to given app instance."""
    app.register_blueprint(workflow_plans_blueprint)
    app.register_blueprint(cache_blueprint)
    app.register_blueprint(config_blueprint)
    app.register_blueprint(dataset_blueprint)
    app.register_blueprint(graph_blueprint)
    app.register_blueprint(jobs_blueprint)
    app.register_blueprint(project_blueprint)
    app.register_blueprint(templates_blueprint)
    app.register_blueprint(version_blueprint)
    app.register_blueprint(apispec_blueprint)


app = create_app()


@app.after_request
def after_request(response):
    """After request handler."""
    service_log.info(
        "{0} {1} {2} {3} {4}".format(
            request.remote_addr, request.method, request.scheme, request.full_path, response.status
        )
    )

    return response


if __name__ == "__main__":
    if len(JWT_TOKEN_SECRET) < 32:
        raise InvalidTokenError("web token must be greater or equal to 32 bytes")

    app.logger.handlers.extend(service_log.handlers)
    app.run()
