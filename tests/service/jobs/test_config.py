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
"""Test config jobs."""
import pytest
from flaky import flaky

from renku.core.errors import MigrationRequired


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_delay_config_set(svc_client_cache, it_remote_repo_url, view_user_data):
    """Test delayed set config endpoint."""
    from renku.service.serializers.config import ConfigSetRequest

    context = ConfigSetRequest().load({"git_url": it_remote_repo_url, "migrate_project": True, "config": {"a": "b"}})

    _, _, cache = svc_client_cache
    renku_module = "renku.service.controllers.config_set"
    renku_ctrl = "SetConfigCtrl"

    user = cache.ensure_user(view_user_data)
    job = cache.make_job(
        user, job_data={"ctrl_context": {**context, "renku_module": renku_module, "renku_ctrl": renku_ctrl}}
    )

    from renku.service.jobs.delayed_ctrl import delayed_ctrl_job

    updated_job = delayed_ctrl_job(context, view_user_data, job.job_id, renku_module, renku_ctrl)

    assert updated_job
    assert {"config", "remote_branch"} == set(updated_job.ctrl_result["result"].keys())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_delay_config_set_failure(svc_client_cache, it_remote_repo_url, view_user_data):
    """Test delayed set config endpoint."""
    from renku.service.serializers.config import ConfigSetRequest

    context = ConfigSetRequest().load({"git_url": it_remote_repo_url, "config": {"a": "b"}})

    _, _, cache = svc_client_cache
    renku_module = "renku.service.controllers.config_set"
    renku_ctrl = "SetConfigCtrl"

    user = cache.ensure_user(view_user_data)
    job = cache.make_job(
        user, job_data={"ctrl_context": {**context, "renku_module": renku_module, "renku_ctrl": renku_ctrl}}
    )

    from renku.service.jobs.delayed_ctrl import delayed_ctrl_job

    with pytest.raises(MigrationRequired):
        delayed_ctrl_job(context, view_user_data, job.job_id, renku_module, renku_ctrl)
