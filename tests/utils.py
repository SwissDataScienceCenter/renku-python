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
import contextlib
import os
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import List, Optional, Union

import pytest
from flaky import flaky
from git import Repo

from renku.core.management.command_builder.command import inject, remove_injector
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.models.dataset import Dataset
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.workflow.plan import Plan


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


def assert_dataset_is_mutated(old: Dataset, new: Dataset, mutator=None):
    """Check metadata is updated correctly after dataset mutation."""
    assert old.name == new.name
    assert old.initial_identifier == new.initial_identifier
    assert old.id != new.id
    assert old.identifier != new.identifier
    assert old.id == new.derived_from.url_id
    assert old.date_created != new.date_created
    assert new.same_as is None
    assert new.date_published is None
    assert new.identifier in new.id

    if mutator:
        old_creators = {c.email for c in old.creators}
        new_creators = {c.email for c in new.creators}
        assert new_creators == old_creators | {mutator.email}, f"{new_creators} {old_creators}"


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


def format_result_exception(result):
    """Format a `runner.invoke` exception result into a nice string repesentation."""

    if getattr(result, "exc_info", None):
        stacktrace = "".join(traceback.format_exception(*result.exc_info))
    else:
        stacktrace = ""

    return f"Stack Trace:\n{stacktrace}\n\nOutput:\n{result.output}"


def load_dataset(name: str) -> Optional[Dataset]:
    """Load dataset from disk."""
    datasets_provenance = DatasetsProvenance()

    return datasets_provenance.get_by_name(name)


@contextmanager
@inject.autoparams("dataset_gateway", "database_dispatcher")
def with_dataset(
    client,
    name: str = None,
    dataset_gateway: IDatasetGateway = None,
    database_dispatcher: IDatabaseDispatcher = None,
    commit_database: bool = False,
):
    """Yield an editable metadata object for a dataset."""
    dataset = client.get_dataset(name=name, strict=True, immutable=True)
    dataset._v_immutable = False

    yield dataset

    if commit_database:
        dataset_gateway.add_or_remove(dataset)
        database_dispatcher.current_database.commit()


def retry_failed(fn=None, extended: bool = False):
    """
    Decorator to run flaky with same number of max and min repetitions across all tests.

    :param extended: allow more repetitions than usual.
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


@contextlib.contextmanager
def injection_manager(bindings):
    """Context manager to temporarly do injections."""

    def _bind(binder):
        for key, value in bindings["bindings"].items():
            binder.bind(key, value)
        for key, value in bindings["constructor_bindings"].items():
            binder.bind_to_constructor(key, value)

        return binder

    inject.configure(_bind, bind_in_runtime=False)
    try:
        yield
    finally:
        try:
            if IDatabaseDispatcher in bindings["bindings"]:
                bindings["bindings"][IDatabaseDispatcher].finalize_dispatcher()
        finally:
            remove_injector()


def write_and_commit_file(repo: Repo, path: Union[Path, str], content: str):
    """Write content to a given file and make a commit."""
    filepath = Path(path)
    path = str(path)

    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(content)

    repo.git.add(path)
    repo.index.commit(f"Updated '{path}'")


def create_dummy_activity(
    plan: Union[Plan, str],
    usages: List[Union[Path, str]] = (),
    generations: List[Union[Path, str]] = (),
    ended_at_time=None,
) -> Activity:
    """Create a dummy activity."""
    if not isinstance(plan, Plan):
        assert isinstance(plan, str)
        plan = Plan(id=Plan.generate_id(), name=plan)

    ended_at_time = ended_at_time or (datetime.utcnow() - timedelta(hours=1))
    checksum = "abc123"

    activity_id = Activity.generate_id()

    return Activity(
        id=activity_id,
        ended_at_time=ended_at_time,
        association=Association(id=Association.generate_id(activity_id), plan=plan),
        generations=[
            Generation(
                id=Generation.generate_id(activity_id),
                entity=Entity(id=Entity.generate_id(checksum, g), checksum=checksum, path=g),
            )
            for g in generations
        ],
        usages=[
            Usage(
                id=Usage.generate_id(activity_id),
                entity=Entity(id=Entity.generate_id(checksum, u), checksum=checksum, path=u),
            )
            for u in usages
        ],
    )
