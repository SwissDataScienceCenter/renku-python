# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 -Swiss Data Science Center (SDSC)
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
"""Test utility functions."""
import os
import traceback
import uuid
from contextlib import contextmanager
from functools import wraps

import pytest
from flaky import flaky

from renku.core.metadata.database import Database
from renku.core.models.dataset import DatasetsProvenance


def raises(error):
    """Wrapper around pytest.raises to support None."""
    if error:
        return pytest.raises(error)
    else:

        @contextmanager
        def not_raises():
            try:
                yield
            except Exception as e:
                raise e

        return not_raises()


def make_dataset_add_payload(project_id, urls, name=None):
    """Make dataset add request payload."""
    files = []
    for url in urls:
        if isinstance(url, tuple):
            files.append({url[0]: url[1]})

        if isinstance(url, str):
            files.append({"file_url": url})

    return {
        "project_id": project_id,
        "name": name or uuid.uuid4().hex,
        "create_dataset": True,
        "force": False,
        "files": files,
    }


def assert_dataset_is_mutated(old, new, mutator=None):
    """Check metadata is updated correctly after dataset mutation."""
    assert old._id != new._id
    assert old.identifier != new.identifier
    assert old.path == new.path
    assert old._id == new.derived_from.url["@id"]
    assert old.date_created != new.date_created
    assert new.same_as is None
    assert new.date_published is None
    assert new.identifier in new._id
    assert new.identifier in new._label
    assert new.identifier in new.url

    if mutator:
        old_creators = {c.email for c in old.creators}
        new_creators = {c.email for c in new.creators}
        assert new_creators == old_creators | {mutator.email}


@contextmanager
def modified_environ(*remove, **update):
    """
    Temporarily updates the ``os.environ`` dictionary in-place.

    The ``os.environ`` dictionary is updated in-place so that the modification
    is sure to work in all situations.
    :param remove: Environment variables to remove.
    :param update: Dictionary of environment variables and values to add/update.
    """
    env = os.environ
    update = update or {}
    remove = remove or []

    # List of environment variables being updated or removed.
    stomped = (set(update.keys()) | set(remove)) & set(env.keys())
    # Environment variables and values to restore on exit.
    update_after = {k: env[k] for k in stomped}
    # Environment variables and values to remove on exit.
    remove_after = frozenset(k for k in update if k not in env)

    try:
        env.update(update)
        [env.pop(k, None) for k in remove]
        yield
    finally:
        env.update(update_after)
        [env.pop(k) for k in remove_after]


def get_datasets_provenance(client) -> DatasetsProvenance:
    """Return DatasetsProvenance for a client."""
    assert client.has_graph_files()

    database = Database.from_path(client.database_path)
    return DatasetsProvenance(database)


def format_result_exception(result):
    """Format a `runner.invoke` exception result into a nice string repesentation."""

    if getattr(result, "exc_info", None):
        stacktrace = "".join(traceback.format_exception(*result.exc_info))
    else:
        stacktrace = ""

    return f"Stack Trace:\n{stacktrace}\n\nOutput:\n{result.output}"


def retry_failed(fn=None, extended: bool = False):
    """
    Decorator to run flaky with same number of max and min repetitions across all tests.
    """

    def decorate(fn):
        limit = 20 if extended else 5

        @flaky(max_runs=limit, min_passes=1)
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    if fn:
        return decorate(fn)
    return decorate
