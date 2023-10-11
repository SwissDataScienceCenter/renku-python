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
"""Renku service project clone tests."""
import json

import pytest
from werkzeug.utils import secure_filename

from renku.ui.service.serializers.headers import encode_b64
from tests.utils import assert_rpc_response, retry_failed


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_service_user_non_existing_project_clone(svc_client_cache, it_remote_repo_url):
    """Check reading manifest template."""
    svc_client, headers, cache = svc_client_cache
    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    # NOTE: clone a valid repo and verify there is one project in the cache
    payload = {"git_url": it_remote_repo_url}
    response = svc_client.post("/project.show", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    projects = list(cache.get_projects(user))
    assert 1 == len(projects)

    # NOTE: invalidate the project
    cache.invalidate_project(user, projects[0].project_id)
    projects = list(cache.get_projects(user))
    assert 0 == len(projects)

    # NOTE: try to clone a non-existing repo and verify no other projects are added to the cache
    payload["git_url"] = f"{it_remote_repo_url}-non-existing-project-url"
    response = svc_client.post("/project.show", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response, "error")
    projects = list(cache.get_projects(user))
    assert 0 == len(projects)
