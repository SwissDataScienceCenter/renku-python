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
"""Renku service config."""
import os
import tempfile
from pathlib import Path

GIT_ACCESS_DENIED_ERROR_CODE = -32000
GIT_UNKNOWN_ERROR_CODE = -32001

RENKU_EXCEPTION_ERROR_CODE = -32100
REDIS_EXCEPTION_ERROR_CODE = -32200

INVALID_HEADERS_ERROR_CODE = -32601
INVALID_PARAMS_ERROR_CODE = -32602
INTERNAL_FAILURE_ERROR_CODE = -32603

SERVICE_NAME = "Renku Service"
OPENAPI_VERSION = "2.0"
API_VERSION = "v1"

SWAGGER_URL = "/api/docs"
API_SPEC_URL = os.getenv("RENKU_SVC_SWAGGER_URL", "/api/{0}/spec".format(API_VERSION))

PROJECT_CLONE_DEPTH_DEFAULT = int(os.getenv("PROJECT_CLONE_DEPTH_DEFAULT", 1))
TEMPLATE_CLONE_DEPTH_DEFAULT = int(os.getenv("TEMPLATE_CLONE_DEPTH_DEFAULT", 0))

CACHE_DIR = os.getenv("CACHE_DIR", os.path.realpath(tempfile.TemporaryDirectory().name))
CACHE_UPLOADS_PATH = Path(CACHE_DIR) / Path("uploads")
CACHE_UPLOADS_PATH.mkdir(parents=True, exist_ok=True)

CACHE_PROJECTS_PATH = Path(CACHE_DIR) / Path("projects")
CACHE_PROJECTS_PATH.mkdir(parents=True, exist_ok=True)

TAR_ARCHIVE_CONTENT_TYPE = "application/x-tar"
ZIP_ARCHIVE_CONTENT_TYPE = "application/zip"

SUPPORTED_ARCHIVES = [
    TAR_ARCHIVE_CONTENT_TYPE,
    ZIP_ARCHIVE_CONTENT_TYPE,
]

SERVICE_PREFIX = os.getenv("CORE_SERVICE_PREFIX", "/")
LOGGER_CONFIG_FILE = Path("renku") / "service" / "logging.yaml"
