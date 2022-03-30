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
import json
import os
import re
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Iterator, List, Optional, Union

import pytest
from flaky import flaky

from renku.core.management.command_builder.command import inject, remove_injector
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.metadata.repository import Repository
from renku.core.models.dataset import Dataset
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.provenance.agent import Person, SoftwareAgent
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
    assert new.derived_from is not None
    assert old.id == new.derived_from.url_id
    if old.date_created and new.date_created:
        assert old.date_created <= new.date_created
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

    Args:
        remove: Environment variables to remove.
        update: Dictionary of environment variables and values to add/update.
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

    try:
        stderr = f"\n{result.stderr}"
    except ValueError:
        stderr = ""

    return f"Stack Trace:\n{stacktrace}\n\nOutput:\n{result.output + stderr}"


def load_dataset(name: str) -> Optional[Dataset]:
    """Load dataset from disk."""
    datasets_provenance = DatasetsProvenance()

    return datasets_provenance.get_by_name(name)


@contextmanager
@inject.autoparams("dataset_gateway", "database_dispatcher")
def with_dataset(
    client,
    *,
    name: str,
    dataset_gateway: IDatasetGateway,
    database_dispatcher: IDatabaseDispatcher,
    commit_database: bool = False,
) -> Iterator[Optional[Dataset]]:
    """Yield an editable metadata object for a dataset."""
    dataset = DatasetsProvenance().get_by_name(name=name, strict=True, immutable=True)

    if not dataset:
        return None

    dataset.unfreeze()

    yield dataset

    if commit_database:
        dataset_gateway.add_or_remove(dataset)
        database_dispatcher.current_database.commit()


def retry_failed(fn=None, extended: bool = False):
    """
    Decorator to run flaky with same number of max and min repetitions across all tests.

    Args:
        fn (Callable): The function to retry.
        extended (bool, optional): allow more repetitions than usual (Default value = False).
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


def write_and_commit_file(repository: Repository, path: Union[Path, str], content: str):
    """Write content to a given file and make a commit."""
    path = repository.path / path

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)

    repository.add(path)
    repository.commit(f"Updated '{path.relative_to(repository.path)}'")


def create_dummy_activity(
    plan: Union[Plan, str],
    usages: List[Union[Path, str]] = [],
    generations: List[Union[Path, str]] = [],
    ended_at_time=None,
) -> Activity:
    """Create a dummy activity."""
    if not isinstance(plan, Plan):
        assert isinstance(plan, str)
        plan = Plan(id=Plan.generate_id(), name=plan, command=plan)

    ended_at_time = ended_at_time or (datetime.utcnow() - timedelta(hours=1))
    checksum = "abc123"

    activity_id = Activity.generate_id()

    return Activity(
        id=activity_id,
        started_at_time=ended_at_time - timedelta(hours=1),
        ended_at_time=ended_at_time,
        agents=[
            SoftwareAgent(name="renku test", id="https://github.com/swissdatasciencecenter/renku-python/tree/test"),
            Person(name="Renkubot", email="test@renkulab.io"),
        ],
        association=Association(
            id=Association.generate_id(activity_id),
            plan=plan,
            agent=SoftwareAgent(
                name="renku test", id="https://github.com/swissdatasciencecenter/renku-python/tree/test"
            ),
        ),
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


def clone_compressed_repository(base_path, name) -> Repository:
    """Decompress and clone a repository."""
    import tarfile

    compressed_repo_path = Path(__file__).parent / "data" / f"{name}.tar.gz"
    working_dir = base_path / name

    bare_base_path = working_dir / "bare"

    with tarfile.open(compressed_repo_path, "r") as fixture:
        fixture.extractall(str(bare_base_path))

    bare_path = bare_base_path / name
    repository_path = working_dir / "repository"
    repository = Repository.clone_from(bare_path, repository_path)

    return repository


def assert_rpc_response(response, with_key="result"):
    """Check rpc result in response."""
    assert response and 200 == response.status_code

    response_text = re.sub(r"http\S+", "", json.dumps(response.json))
    assert with_key in response_text
