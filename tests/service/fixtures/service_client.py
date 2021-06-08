# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku service fixtures for client interactions."""
import os
import time
import urllib
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def real_sync():
    """Enable remote sync."""
    import importlib

    from renku.core.commands import save

    # NOTE: Use this fixture only in serial tests. save.repo_sync is mocked; reloading the save module to undo the mock.
    importlib.reload(save)


@pytest.fixture()
def svc_client(mock_redis, svc_cache_dir):
    """Renku service client."""
    from renku.service.entrypoint import create_app

    flask_app = create_app()

    testing_client = flask_app.test_client()
    testing_client.testing = True

    ctx = flask_app.app_context()
    ctx.push()

    yield testing_client

    ctx.pop()


@pytest.fixture(scope="function")
def svc_cache_dir(mocker, tmpdir):
    """Mock temporary dir for cache."""
    import renku.service.cache.models.file
    import renku.service.cache.models.project
    import renku.service.config
    import renku.service.controllers.cache_files_upload
    import renku.service.controllers.datasets_create
    import renku.service.controllers.datasets_edit
    import renku.service.entrypoint
    import renku.service.utils

    project_dir = Path(tmpdir.mkdir("projects"))
    upload_dir = Path(tmpdir.mkdir("uploads"))

    mocker.patch.object(renku.service.config, "CACHE_DIR", Path(tmpdir))
    mocker.patch.object(renku.service.entrypoint, "CACHE_DIR", Path(tmpdir))
    mocker.patch.object(renku.service.config, "CACHE_UPLOADS_PATH", upload_dir)
    mocker.patch.object(renku.service.cache.models.project, "CACHE_PROJECTS_PATH", project_dir)
    mocker.patch.object(renku.service.utils, "CACHE_PROJECTS_PATH", project_dir)
    mocker.patch.object(renku.service.utils, "CACHE_UPLOADS_PATH", upload_dir)
    mocker.patch.object(renku.service.cache.models.file, "CACHE_UPLOADS_PATH", upload_dir)
    mocker.patch.object(renku.service.controllers.cache_files_upload, "CACHE_UPLOADS_PATH", upload_dir)
    mocker.patch.object(renku.service.controllers.datasets_create, "CACHE_UPLOADS_PATH", upload_dir)
    mocker.patch.object(renku.service.controllers.datasets_edit, "CACHE_UPLOADS_PATH", upload_dir)

    yield


@pytest.fixture(scope="function")
def svc_client_cache(mock_redis, identity_headers, svc_cache_dir):
    """Service jobs fixture."""
    from renku.service.entrypoint import create_app

    flask_app = create_app()

    testing_client = flask_app.test_client()
    testing_client.testing = True

    ctx = flask_app.app_context()
    ctx.push()

    yield testing_client, identity_headers, flask_app.config.get("cache")

    ctx.pop()


@pytest.fixture(scope="module")
def identity_headers():
    """Get authentication headers."""
    import jwt

    from renku.service.serializers.headers import JWT_TOKEN_SECRET

    jwt_data = {
        "jti": "12345",
        "exp": int(time.time()) + 1e6,
        "nbf": 0,
        "iat": 1595317694,
        "iss": "https://stable.dev.renku.ch/auth/realms/Renku",
        "aud": ["renku"],
        "sub": "9ab2fc80-3a5c-426d-ae78-56de01d214df",
        "typ": "ID",
        "azp": "renku",
        "nonce": "12345",
        "auth_time": 1595317694,
        "session_state": "12345",
        "acr": "1",
        "email_verified": False,
        "preferred_username": "andi@bleuler.com",
        "given_name": "Andreas",
        "family_name": "Bleuler",
        "name": "Andreas Bleuler",
        "email": "andi@bleuler.com",
    }

    headers = {
        "Content-Type": "application/json",
        "Renku-User": jwt.encode(jwt_data, JWT_TOKEN_SECRET, algorithm="HS256"),
        "Authorization": "Bearer {0}".format(os.getenv("IT_OAUTH_GIT_TOKEN")),
    }

    return headers


@pytest.fixture(scope="module")
def view_user_data(identity_headers):
    """View user data object."""
    from renku.service.serializers.headers import RequiredIdentityHeaders

    user_data = RequiredIdentityHeaders().load(identity_headers)

    return user_data


@pytest.fixture(scope="module")
def authentication_headers_raw():
    """Get authentication headers without renku user identification."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {0}".format(os.getenv("IT_OAUTH_GIT_TOKEN")),
    }

    return headers


@pytest.fixture(scope="module")
def authentication_headers(authentication_headers_raw):
    """Get authentication headers."""
    identification = {
        "Renku-User-Id": "b4b4de0eda0f471ab82702bd5c367fa7",
        "Renku-User-FullName": "Just Sam",
        "Renku-User-Email": "contact@renkulab.io",
    }

    return {**authentication_headers_raw, **identification}


@pytest.fixture
def svc_client_with_user(svc_client_cache):
    """Service client with a predefined user."""
    from werkzeug.utils import secure_filename

    from renku.service.serializers.headers import encode_b64

    svc_client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    yield svc_client, headers, cache, user


@pytest.fixture
def svc_synced_client(svc_client_with_user, real_sync):
    """Renku service client with remote sync."""
    yield svc_client_with_user


@pytest.fixture
def svc_client_with_templates(svc_client, mock_redis, identity_headers, template):
    """Setup and teardown steps for templates tests."""

    yield svc_client, identity_headers, template


@pytest.fixture
def svc_client_templates_creation(svc_client_with_templates):
    """Setup and teardown steps for templates tests."""
    from renku.core.utils.requests import retry
    from renku.core.utils.scm import normalize_to_ascii

    svc_client, authentication_headers, template = svc_client_with_templates
    parameters = []
    for parameter in template["metadata"].keys():
        parameters.append({"key": parameter, "value": template["metadata"][parameter]})

    payload = {
        **template,
        "identifier": template["id"],
        "parameters": parameters,
        "project_name": f"Test renku-core {uuid.uuid4().hex[:12]}",
        "project_namespace": "renku-python-integration-tests",
        "project_repository": "https://dev.renku.ch/gitlab",
    }

    # clenup by invoking the GitLab delete API
    # TODO: consider using the project delete endpoint once implemented
    def remove_project():
        project_slug = "{0}/{1}".format(payload["project_namespace"], normalize_to_ascii(payload["project_name"]))

        project_slug_encoded = urllib.parse.quote(project_slug, safe="")
        project_delete_url = "{0}/api/v4/projects/{1}".format(payload["project_repository"], project_slug_encoded)

        with retry() as session:
            session.delete(url=project_delete_url, headers=authentication_headers)

        return True

    yield svc_client, authentication_headers, payload, remove_project
