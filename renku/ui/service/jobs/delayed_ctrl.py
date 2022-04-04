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
"""Delayed controller jobs."""
import importlib

from renku.ui.service.logger import worker_log
from renku.ui.service.views.decorators import requires_cache


def to_ctrl(cache, context, user_data, renku_module, renku_ctrl):
    """Based on renku operation return valid controller."""
    ctrl_module = importlib.import_module(renku_module)
    cls = getattr(ctrl_module, renku_ctrl)

    context.pop("is_delayed", None)  # NOTE: Prevent recursive invocation.
    migrate_project = context.pop("migrate_project", False)
    return cls(cache, user_data, context, migrate_project=migrate_project)


@requires_cache
def delayed_ctrl_job(cache, context, user_data, job_id, renku_module, renku_ctrl):
    """Delayed controller job."""
    ctrl = to_ctrl(cache, context, user_data, renku_module, renku_ctrl)
    result = ctrl.to_response().json

    user = cache.ensure_user(user_data)
    worker_log.debug(f"executing delayed job {renku_ctrl} for {user.user_id}:{user.fullname}")

    user_job = cache.get_job(user, job_id)
    user_job.ctrl_result = result
    user_job.save()

    return user_job
