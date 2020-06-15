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

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask import Flask, request
from flask_apispec import FlaskApiSpec
from flask_swagger_ui import get_swaggerui_blueprint

from renku.service.cache import cache
from renku.service.config import API_SPEC_URL, API_VERSION, CACHE_DIR, \
    OPENAPI_VERSION, SERVICE_NAME, SWAGGER_URL
from renku.service.logger import service_log
from renku.service.utils.json_encoder import SvcJSONEncoder
from renku.service.views.cache import CACHE_BLUEPRINT_TAG, cache_blueprint, \
    list_projects_view, list_uploaded_files_view, migrate_project_view, \
    project_clone_view, upload_file_view
from renku.service.views.datasets import DATASET_BLUEPRINT_TAG, \
    add_file_to_dataset_view, create_dataset_view, dataset_blueprint, \
    edit_dataset_view, import_dataset_view, list_dataset_files_view, \
    list_datasets_view
from renku.service.views.jobs import JOBS_BLUEPRINT_TAG, jobs_blueprint, \
    list_jobs
from renku.service.views.templates import TEMPLATES_BLUEPRINT_TAG, \
    read_manifest_from_template, templates_blueprint

logging.basicConfig(level=os.getenv('SERVICE_LOG_LEVEL', 'WARNING'))


def create_app():
    """Creates a Flask app with a necessary configuration."""
    app = Flask(__name__)
    app.secret_key = os.getenv('RENKU_SVC_SERVICE_KEY', uuid.uuid4().hex)
    app.json_encoder = SvcJSONEncoder
    app.config['UPLOAD_FOLDER'] = CACHE_DIR

    max_content_size = os.getenv('MAX_CONTENT_LENGTH')
    if max_content_size:
        app.config['MAX_CONTENT_LENGTH'] = max_content_size

    app.config['cache'] = cache

    build_routes(app)

    @app.route('/health')
    def health():
        import renku
        return 'renku repository service version {}\n'.format(
            renku.__version__
        )

    return app


def build_routes(app):
    """Register routes to given app instance."""
    app.config.update({
        'APISPEC_SPEC':
            APISpec(
                title=SERVICE_NAME,
                openapi_version=OPENAPI_VERSION,
                version=API_VERSION,
                plugins=[MarshmallowPlugin()],
            ),
        'APISPEC_SWAGGER_URL': API_SPEC_URL,
    })
    app.register_blueprint(cache_blueprint)
    app.register_blueprint(dataset_blueprint)
    app.register_blueprint(jobs_blueprint)
    app.register_blueprint(templates_blueprint)

    swaggerui_blueprint = get_swaggerui_blueprint(
        SWAGGER_URL, API_SPEC_URL, config={'app_name': 'Renku Service'}
    )
    app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

    docs = FlaskApiSpec(app)

    docs.register(list_uploaded_files_view, blueprint=CACHE_BLUEPRINT_TAG)
    docs.register(upload_file_view, blueprint=CACHE_BLUEPRINT_TAG)
    docs.register(project_clone_view, blueprint=CACHE_BLUEPRINT_TAG)
    docs.register(list_projects_view, blueprint=CACHE_BLUEPRINT_TAG)
    docs.register(migrate_project_view, blueprint=CACHE_BLUEPRINT_TAG)

    docs.register(list_datasets_view, blueprint=DATASET_BLUEPRINT_TAG)
    docs.register(list_dataset_files_view, blueprint=DATASET_BLUEPRINT_TAG)
    docs.register(add_file_to_dataset_view, blueprint=DATASET_BLUEPRINT_TAG)
    docs.register(create_dataset_view, blueprint=DATASET_BLUEPRINT_TAG)
    docs.register(import_dataset_view, blueprint=DATASET_BLUEPRINT_TAG)
    docs.register(edit_dataset_view, blueprint=DATASET_BLUEPRINT_TAG)

    docs.register(list_jobs, blueprint=JOBS_BLUEPRINT_TAG)
    docs.register(
        read_manifest_from_template, blueprint=TEMPLATES_BLUEPRINT_TAG
    )


app = create_app()


@app.after_request
def after_request(response):
    """After request handler."""
    service_log.info(
        '{0} {1} {2} {3} {4}'.format(
            request.remote_addr, request.method, request.scheme,
            request.full_path, response.status
        )
    )

    return response


@app.errorhandler(Exception)
def exceptions(e):
    """App exception logger."""
    tb = traceback.format_exc()
    service_log.error(
        '{} {} {} {} 5xx INTERNAL SERVER ERROR\n{}'.format(
            request.remote_addr, request.method, request.scheme,
            request.full_path, tb
        )
    )

    return e.status_code


if __name__ == '__main__':
    app.logger.handlers.extend(service_log.handlers)
    app.run()
