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
"""Renku graph jobs."""
import tempfile
from urllib.parse import urlparse

from git import GitError, Repo
from marshmallow import EXCLUDE
from requests import RequestException
from sentry_sdk import capture_exception

from renku.core.commands.format.graph import jsonld
from renku.core.commands.graph import build_graph
from renku.core.commands.migrate import migrate_project
from renku.core.errors import RenkuException
from renku.core.utils.contexts import chdir
from renku.core.utils.requests import retry
from renku.service.serializers.cache import ProjectCloneContext
from renku.service.serializers.graph import GraphBuildCallbackError, GraphBuildCallbackSuccess


def report_recoverable(payload, exception, callback_url):
    """Report to callback URL recoverable state."""
    capture_exception(exception)

    if not callback_url:
        return

    payload["failure"] = {"type": "RECOVERABLE_FAILURE", "message": str(exception)}

    data = GraphBuildCallbackError().load(payload)
    with retry() as session:
        session.post(callback_url, data=data)


def report_unrecoverable(payload, exception, callback_url):
    """Report to callback URL unrecoverable state."""
    capture_exception(exception)

    if not callback_url:
        return

    payload["failure"] = {"type": "UNRECOVERABLE_FAILURE", "message": str(exception)}

    data = GraphBuildCallbackError().load(payload)
    with retry() as session:
        session.post(callback_url, data=data)


def report_success(request_payload, graph_payload, callback_url):
    """Report to callback URL success state."""
    data = GraphBuildCallbackSuccess().load({**request_payload, **graph_payload})

    if not callback_url:
        return data

    with retry() as session:
        session.post(callback_url, data=data)

    return data


def graph_build_job(revision, git_url, callback_url, token, timeout=None):
    """Build graph and post triples to callback URL."""
    if not urlparse(callback_url).geturl():
        raise RuntimeError("invalid callback_url")

    ctx = ProjectCloneContext().load({"git_url": git_url, "token": token}, unknown=EXCLUDE)
    callback_payload = {
        "project_url": git_url,
        "commit_id": revision or "HEAD",
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            Repo.clone_from(ctx["url_with_auth"], tmpdir)
        except GitError as e:
            report_recoverable(callback_payload, e, callback_url)

        with chdir(tmpdir):
            try:
                migrate_project()

                graph = build_graph(revision=callback_payload["commit_id"])
                graph_payload = {"payload": jsonld(graph, strict=True, to_stdout=False)}

                data = report_success(callback_payload, graph_payload, callback_url)

            except (RequestException, RenkuException, MemoryError) as e:
                report_recoverable(callback_payload, e, callback_url)

            except BaseException as e:
                report_unrecoverable(callback_payload, e, callback_url)

        return data or callback_payload
