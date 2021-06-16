# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 - Swiss Data Science Center (SDSC)
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
"""Renku service project related job tests."""
import pytest
from flaky import flaky


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_delay_migration_job(svc_client_cache, it_remote_repo_url_temp_branch, view_user_data):
    """Unlink a file from a dataset failure."""
    from renku.service.serializers.cache import ProjectMigrateRequest

    it_remote_repo_url, branch = it_remote_repo_url_temp_branch

    context = ProjectMigrateRequest().load(
        {"git_url": it_remote_repo_url, "migrate_project": True, "ref": branch, "skip_docker_update": True}
    )

    _, _, cache = svc_client_cache
    renku_module = "renku.service.controllers.cache_migrate_project"
    renku_ctrl = "MigrateProjectCtrl"

    user = cache.ensure_user(view_user_data)
    job = cache.make_job(
        user, job_data={"ctrl_context": {**context, "renku_module": renku_module, "renku_ctrl": renku_ctrl}}
    )

    from renku.service.jobs.delayed_ctrl import delayed_ctrl_job

    updated_job = delayed_ctrl_job(context, view_user_data, job.job_id, renku_module, renku_ctrl)
    assert updated_job
    assert {"docker_migrated", "was_migrated", "template_migrated", "messages"} == updated_job.ctrl_result[
        "result"
    ].keys()
