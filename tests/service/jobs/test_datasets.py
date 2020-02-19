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
"""Renku service dataset jobs tests."""
import os
import shutil
import uuid
from pathlib import Path

import pytest

from renku.core.commands.dataset import list_datasets
from renku.core.utils.contexts import chdir
from renku.service.jobs.constants import USER_JOB_STATE_COMPLETED, \
    USER_JOB_STATE_ENQUEUED
from renku.service.jobs.datasets import dataset_import
from renku.service.utils import make_project_path


@pytest.mark.parametrize(
    'doi', [
        '10.5281/zenodo.3239980',
        '10.5281/zenodo.3188334',
        '10.7910/DVN/TJCLKP',
    ]
)
@pytest.mark.integration
def test_dataset_import_job(doi, svc_client_cache, project):
    """Test dataset import"""
    client, cache = svc_client_cache

    user = {'user_id': 'user'}

    project_meta = {
        'project_id': uuid.uuid4().hex,
        'name': Path(project).name,
        'fullname': 'full project name',
        'email': 'my@email.com',
        'owner': 'me',
        'token': 'awesome token',
        'git_url': 'git@gitlab.com'
    }

    job_request = {
        'job_id': uuid.uuid4().hex,
        'state': USER_JOB_STATE_ENQUEUED,
    }

    cache.create_job(user, job_request)
    cache.set_project(user, project_meta['project_id'], project_meta)

    dest = make_project_path(user, project_meta)
    os.makedirs(dest.parent, exist_ok=True)
    if not (project / dest).exists():
        shutil.copytree(project, dest)

    dataset_import(
        user,
        job_request['job_id'],
        project_meta['project_id'],
        doi,
    )

    with chdir(dest):
        # TODO: #1022
        result = list_datasets(None, 'data', 'json-ld')
        assert result
        assert doi in result

    updated_job = cache.get_job(user, job_request['job_id'])

    assert USER_JOB_STATE_COMPLETED == updated_job['state']
    assert {
        'extras',
        'job_id',
        'state',
        'created_at',
        'updated_at',
    } == set(updated_job.keys())
