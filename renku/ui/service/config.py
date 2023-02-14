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

try:
    import importlib_resources  # type: ignore[import]
except ImportError:
    import importlib.resources as importlib_resources  # type: ignore

SVC_ERROR_GENERIC = 0
SVC_ERROR_USER = 1000
SVC_ERROR_PROGRAMMING = 2000
SVC_ERROR_INTERMITTENT = 3000

DOCS_URL_BASE = "https://renku-python.readthedocs.io/en/latest/"
DOCS_URL_ERRORS = "service_errors.html#renku.ui.service.errors."
ERROR_NOT_AVAILABLE = "Unknown"

SERVICE_NAME = "Renku Service"
OPENAPI_VERSION = "3.0.3"
API_VERSION = "v1"

PROJECT_CLONE_NO_DEPTH = -1
PROJECT_CLONE_DEPTH_DEFAULT = int(os.getenv("PROJECT_CLONE_DEPTH_DEFAULT", 1))
TEMPLATE_CLONE_DEPTH_DEFAULT = int(os.getenv("TEMPLATE_CLONE_DEPTH_DEFAULT", 0))

# NOTE: Defaults to 1GB
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 1073741824))

CACHE_DIR = os.getenv("CACHE_DIR", os.path.realpath(tempfile.TemporaryDirectory().name))
CACHE_UPLOADS_PATH = Path(CACHE_DIR) / Path("uploads")
CACHE_UPLOADS_PATH.mkdir(parents=True, exist_ok=True)

CACHE_PROJECTS_PATH = Path(CACHE_DIR) / Path("projects")
CACHE_PROJECTS_PATH.mkdir(parents=True, exist_ok=True)

TAR_ARCHIVE_CONTENT_TYPE = "application/x-tar"
ZIP_ARCHIVE_CONTENT_TYPE = "application/zip"
GZ_ARCHIVE_CONTENT_TYPE = "application/x-gzip"

SUPPORTED_ARCHIVES = [
    TAR_ARCHIVE_CONTENT_TYPE,
    ZIP_ARCHIVE_CONTENT_TYPE,
    GZ_ARCHIVE_CONTENT_TYPE,
]

# the path prefix on the service
SERVICE_PREFIX = os.getenv("CORE_SERVICE_PREFIX", "/")
# the reverse proxy prefix
SERVICE_API_BASE_PATH = os.getenv("CORE_SERVICE_API_BASE_PATH", "/")
# path to the swagger spec
API_SPEC_URL = SERVICE_PREFIX.lstrip("/") + "/spec.json"
# URL for fetching the OIDC configuration
OIDC_URL = os.getenv("OIDC_URL", "/auth/realms/Renku/.well-known/openid-configuration")

ref = importlib_resources.files("renku.ui.service") / "logging.yaml"
with importlib_resources.as_file(ref) as path:
    LOGGER_CONFIG_FILE = path
MESSAGE_PREFIX = os.environ.get("RENKU_DOMAIN", "service") + ":"

# Sentry configuration
SENTRY_ENABLED = os.getenv("SENTRY_ENABLED", "false").lower() == "true"
SENTRY_SAMPLERATE = float(os.getenv("SENTRY_SAMPLE_RATE", 0.2))
