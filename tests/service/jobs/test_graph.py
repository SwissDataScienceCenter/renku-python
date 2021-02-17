# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
"""Renku service graph jobs tests."""
import json

import pytest

from conftest import IT_GIT_ACCESS_TOKEN, IT_REMOTE_REPO_URL
from renku.service.jobs.graph import graph_build_job


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job():
    """Test graph build job."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
        "revision": "HEAD",
        "callback_url": "https://webhook.site",
        "token": IT_GIT_ACCESS_TOKEN,
    }

    data = graph_build_job(**payload)
    assert {"project_url", "commit_id", "payload"} == set(data.keys())

    json_ld = json.loads(data["payload"])
    assert json_ld


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job_no_callback():
    """Test graph build job with missing callback."""
    payload = {"git_url": IT_REMOTE_REPO_URL, "revision": "HEAD", "callback_url": "", "token": IT_GIT_ACCESS_TOKEN}

    with pytest.raises(RuntimeError):
        graph_build_job(**payload)


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job_no_revision():
    """Test graph build job with missing revision."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
        "revision": None,
        "callback_url": "https://webhook.site",
        "token": IT_GIT_ACCESS_TOKEN,
    }

    data = graph_build_job(**payload)
    assert {"project_url", "commit_id", "payload"} == set(data.keys())

    json_ld = json.loads(data["payload"])
    assert json_ld


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job_git_url():
    """Test graph build job with missing git url."""
    payload = {"git_url": None, "revision": "HEAD", "callback_url": "", "token": IT_GIT_ACCESS_TOKEN}

    with pytest.raises(RuntimeError):
        graph_build_job(**payload)


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job_missing_token():
    """Test graph build job with missing token."""
    payload = {"git_url": IT_REMOTE_REPO_URL, "revision": "HEAD", "callback_url": "", "token": None}

    with pytest.raises(RuntimeError):
        graph_build_job(**payload)


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
def test_graph_build_job_invalid_token():
    """Test graph build job with wrong token."""
    payload = {"git_url": IT_REMOTE_REPO_URL, "revision": "HEAD", "callback_url": "", "token": "123"}

    with pytest.raises(RuntimeError):
        graph_build_job(**payload)
