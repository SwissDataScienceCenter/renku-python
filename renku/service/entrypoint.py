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
from flask import Flask, jsonify, request, url_for
from jwt import InvalidTokenError
from sentry_sdk import capture_exception
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.rq import RqIntegration

from renku.service.cache import cache
from renku.service.config import CACHE_DIR, HTTP_SERVER_ERROR, SERVICE_PREFIX
from renku.service.logger import service_log
from renku.service.serializers.headers import JWT_TOKEN_SECRET
from renku.service.utils.json_encoder import SvcJSONEncoder
from renku.service.views import error_response
from renku.service.views.apispec import apispec_blueprint
from renku.service.views.cache import cache_blueprint
from renku.service.views.config import config_blueprint
from renku.service.views.datasets import dataset_blueprint
from renku.service.views.graph import graph_blueprint
from renku.service.views.jobs import jobs_blueprint
from renku.service.views.project import project_blueprint
from renku.service.views.templates import templates_blueprint
from renku.service.views.version import version_blueprint

logging.basicConfig(level=os.getenv("SERVICE_LOG_LEVEL", "WARNING"))

if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        environment=os.getenv("SENTRY_ENV"),
        traces_sample_rate=float(os.getenv("SENTRY_SAMPLE_RATE", 0.2)),
        integrations=[FlaskIntegration(), RqIntegration(), RedisIntegration()],
    )


def create_app():
    """Creates a Flask app with a necessary configuration."""
    app = Flask(__name__)
    app.secret_key = os.getenv("RENKU_SVC_SERVICE_KEY", uuid.uuid4().hex)
    app.json_encoder = SvcJSONEncoder
    app.config["UPLOAD_FOLDER"] = CACHE_DIR

    max_content_size = os.getenv("MAX_CONTENT_LENGTH")
    if max_content_size:
        app.config["MAX_CONTENT_LENGTH"] = max_content_size

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

    return app


def build_routes(app):
    """Register routes to given app instance."""
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


@app.errorhandler(Exception)
def exceptions(e):
    """This exceptions handler manages Flask/Werkzeug exceptions.

    For Renku exception handlers check ``service/decorators.py``
    """

    # NOTE: Capture werkzeug exceptions and propagate them to sentry.
    capture_exception(e)

    # NOTE: Capture traceback for dumping it to the log.
    tb = traceback.format_exc()

    if hasattr(e, "code") and e.code == 404:
        service_log.error(
            "{} {} {} {} 404 NOT FOUND\n{}".format(
                request.remote_addr, request.method, request.scheme, request.full_path, tb
            )
        )
        return error_response(HTTP_SERVER_ERROR - e.code, e.name)

    if hasattr(e, "code") and e.code >= 500:
        service_log.error(
            "{} {} {} {} 5xx INTERNAL SERVER ERROR\n{}".format(
                request.remote_addr, request.method, request.scheme, request.full_path, tb
            )
        )
        return error_response(HTTP_SERVER_ERROR - e.code, e.name)

    # NOTE: Werkzeug exceptions should be covered above, following line is for unexpected HTTP server errors.
    return error_response(HTTP_SERVER_ERROR, str(e))


app.debug = os.environ.get("DEBUG_MODE", "false") == "true"

if app.debug:
    import ptvsd

    service_log.debug("Registered routes:")
    for rule in app.url_map.iter_rules():
        service_log.debug(rule)

    ptvsd.enable_attach()
    app.logger.setLevel(logging.DEBUG)
    app.logger.debug("debug mode enabled")

if __name__ == "__main__":
    if len(JWT_TOKEN_SECRET) < 32:
        raise InvalidTokenError("web token must be greater or equal to 32 bytes")

    app.logger.handlers.extend(service_log.handlers)
    app.run()
