#
# Copyright 2020-2023 -Swiss Data Science Center (SDSC)
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

import itertools
import os
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Iterable, Iterator, List, Optional, Tuple, Type, Union

import pytest
from cwltool.context import LoadingContext
from cwltool.load_tool import load_tool
from cwltool.resolver import tool_resolver
from cwltool.workflow import default_make_tool
from flaky import flaky

from renku.command.command_builder.command import inject, replace_injection
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.util.datetime8601 import local_now
from renku.domain_model.dataset import Dataset
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.datasets_provenance import DatasetsProvenance
    from renku.domain_model.provenance.activity import Activity, Generation, Usage
    from renku.domain_model.workflow.plan import Plan
    from renku.infrastructure.repository import Repository


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


def make_dataset_add_payload(git_url, urls, name=None):
    """Make dataset add request payload."""
    files = []
    for url in urls:
        if isinstance(url, tuple):
            files.append({url[0]: url[1]})

        if isinstance(url, str):
            files.append({"file_url": url})

    return {
        "git_url": git_url,
        "slug": name or uuid.uuid4().hex,
        "create_dataset": True,
        "force": False,
        "files": files,
    }


def assert_dataset_is_mutated(old: "Dataset", new: "Dataset", mutator=None):
    """Check metadata is updated correctly after dataset mutation."""
    assert old.name == new.name
    assert old.initial_identifier == new.initial_identifier
    assert old.id != new.id
    assert old.identifier != new.identifier
    assert new.derived_from is not None
    assert old.id == new.derived_from.url_id
    if old.date_created:
        assert old.date_created == new.date_created
    if old.date_published:
        assert old.date_published == new.date_published
    assert new.same_as is None
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
    remove_set = set(remove or [])

    # List of environment variables being updated or removed.
    stomped = (set(update.keys()) | remove_set) & set(env.keys())
    # Environment variables and values to restore on exit.
    update_after = {k: env[k] for k in stomped}
    # Environment variables and values to remove on exit.
    remove_after = frozenset(k for k in update if k not in env)

    try:
        env.update(update)
        [env.pop(k, None) for k in remove_set]
        yield
    finally:
        env.update(update_after)
        [env.pop(k) for k in remove_after]


def format_result_exception(result):
    """Format a `runner.invoke` exception result into a nice string representation."""

    if getattr(result, "exc_info", None):
        stacktrace = "".join(traceback.format_exception(*result.exc_info))
    else:
        stacktrace = ""

    try:
        stderr = f"\n{result.stderr}"
    except ValueError:
        stderr = ""

    return f"Stack Trace:\n{stacktrace}\n\nOutput:\n{result.output + stderr}"


def load_dataset(name: str) -> Optional["Dataset"]:
    """Load dataset from disk."""
    from renku.core.dataset.datasets_provenance import DatasetsProvenance

    datasets_provenance = DatasetsProvenance()

    return datasets_provenance.get_by_slug(name)


def get_test_bindings() -> Tuple[Dict, Dict[Type, Callable[[], Any]]]:
    """Return all possible bindings."""
    from renku.core.interface.activity_gateway import IActivityGateway
    from renku.core.interface.database_gateway import IDatabaseGateway
    from renku.core.interface.dataset_gateway import IDatasetGateway
    from renku.core.interface.plan_gateway import IPlanGateway
    from renku.core.interface.project_gateway import IProjectGateway
    from renku.infrastructure.gateway.activity_gateway import ActivityGateway
    from renku.infrastructure.gateway.database_gateway import DatabaseGateway
    from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
    from renku.infrastructure.gateway.plan_gateway import PlanGateway
    from renku.infrastructure.gateway.project_gateway import ProjectGateway

    constructor_bindings = {
        IPlanGateway: lambda: PlanGateway(),
        IActivityGateway: lambda: ActivityGateway(),
        IDatabaseGateway: lambda: DatabaseGateway(),
        IDatasetGateway: lambda: DatasetGateway(),
        IProjectGateway: lambda: ProjectGateway(),
    }

    return {}, constructor_bindings


def get_dataset_with_injection(name: str) -> Optional["Dataset"]:
    """Load dataset method with injection setup."""
    bindings, constructor_bindings = get_test_bindings()

    with replace_injection(bindings=bindings, constructor_bindings=constructor_bindings):
        return load_dataset(name)


@contextmanager
def get_datasets_provenance_with_injection() -> Generator["DatasetsProvenance", None, None]:
    """Yield an instance  of DatasetsProvenance with injection setup."""
    from renku.core.dataset.datasets_provenance import DatasetsProvenance

    bindings, constructor_bindings = get_test_bindings()

    with replace_injection(bindings=bindings, constructor_bindings=constructor_bindings):
        yield DatasetsProvenance()


@contextmanager
@inject.autoparams("dataset_gateway")
def with_dataset(
    *,
    name: str,
    dataset_gateway: IDatasetGateway,
    commit_database: bool = False,
) -> Iterator[Optional["Dataset"]]:
    """Yield an editable metadata object for a dataset."""
    from renku.core.dataset.datasets_provenance import DatasetsProvenance

    dataset = DatasetsProvenance().get_by_slug(slug=name, strict=True, immutable=True)

    if not dataset:
        return None

    dataset.unfreeze()

    yield dataset

    if commit_database:
        dataset_gateway.add_or_remove(dataset)
        project_context.database.commit()


def retry_failed(fn=None, extended: bool = False):
    """
    Decorator to run flaky with same number of max and min repetitions across all tests.

    Args:
        fn (Callable): The function to retry.
        extended (bool, optional): allow more repetitions than usual (Default value = False).
    """

    def decorate():
        limit = 20 if extended else 5

        @flaky(max_runs=limit, min_passes=1)
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    return decorate() if fn else decorate


def write_and_commit_file(repository: "Repository", path: Union[Path, str], content: str, commit: bool = True):
    """Write content to a given file and make a commit."""
    path = repository.path / path

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)

    repository.add(path)
    if commit:
        repository.commit(f"Updated '{path.relative_to(repository.path)}'", no_verify=True)


def delete_and_commit_file(repository: "Repository", path: Union[Path, str]):
    """Delete a file and make a commit."""
    path = repository.path / path

    path.unlink()

    repository.add(path)
    repository.commit(f"Deleted '{path.relative_to(repository.path)}'")


def create_and_commit_files(repository: "Repository", *path_and_content: Union[Path, str, Tuple[str, str]]):
    """Write content to a given file and make a commit."""
    for file in path_and_content:
        if isinstance(file, (Path, str)):
            write_and_commit_file(repository, path=file, content="", commit=False)
        else:
            assert isinstance(file, tuple) and len(file) == 2, f"Invalid path/content: {file}"
            path, content = file
            write_and_commit_file(repository, path=path, content=content, commit=False)

    repository.commit("Created files")


def create_dummy_activity(
    plan: Union["Plan", str],
    *,
    started_at_time=None,
    ended_at_time=None,
    generations: Iterable[Union[Path, str, "Generation", Tuple[str, str]]] = (),
    id: Optional[str] = None,
    index: Optional[int] = None,
    parameters: Dict[str, Any] = None,
    usages: Iterable[Union[Path, str, "Usage", Tuple[str, str]]] = (),
) -> "Activity":
    """Create a dummy activity."""
    from renku.domain_model.entity import Entity
    from renku.domain_model.provenance.activity import Activity, Association, Generation, Usage
    from renku.domain_model.provenance.agent import Person, SoftwareAgent
    from renku.domain_model.provenance.parameter import ParameterValue
    from renku.domain_model.workflow.plan import Plan
    from renku.infrastructure.repository import Repository

    assert id is None or index is None, "Cannot set both 'id' and 'index'"

    if not isinstance(plan, Plan):
        assert isinstance(plan, str)
        plan = Plan(id=Plan.generate_id(), name=plan, command=plan)

    started_at_time = started_at_time or local_now()
    ended_at_time = ended_at_time or local_now()
    empty_checksum = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"  # Git hash of an empty string/file
    activity_id = id or Activity.generate_id(uuid=None if index is None else str(index))

    parameters_ids = {p.name: p.id for p in itertools.chain(plan.inputs, plan.outputs, plan.parameters)}
    parameters = parameters or {}

    def create_generation(generation) -> Generation:
        if isinstance(generation, Generation):
            return generation
        elif isinstance(generation, (Path, str)):
            entity = Entity(id=Entity.generate_id(empty_checksum, generation), checksum=empty_checksum, path=generation)
            return Generation(id=Generation.generate_id(activity_id), entity=entity)
        else:
            assert isinstance(generation, tuple) and len(generation) == 2, f"Invalid generation: {generation}"
            path, content = generation
            checksum = Repository.hash_string(content=content)
            entity = Entity(id=Entity.generate_id(checksum, path), checksum=checksum, path=path)
            return Generation(id=Generation.generate_id(activity_id), entity=entity)

    def create_usage(usage) -> Usage:
        if isinstance(usage, Usage):
            return usage
        elif isinstance(usage, (Path, str)):
            entity = Entity(id=Entity.generate_id(empty_checksum, usage), checksum=empty_checksum, path=usage)
            return Usage(id=Usage.generate_id(activity_id), entity=entity)
        else:
            assert isinstance(usage, tuple) and len(usage) == 2, f"Invalid usage: {usage}"
            path, content = usage
            checksum = Repository.hash_string(content=content)
            entity = Entity(id=Entity.generate_id(checksum, path), checksum=checksum, path=path)
            return Usage(id=Usage.generate_id(activity_id), entity=entity)

    return Activity(
        id=activity_id,
        started_at_time=started_at_time,
        ended_at_time=ended_at_time,
        agents=[
            SoftwareAgent(name="renku test", id="https://github.com/swissdatasciencecenter/renku-python/tree/test"),
            Person(name="Renku-bot", email="test@renkulab.io"),
        ],
        association=Association(
            id=Association.generate_id(activity_id),
            plan=plan,
            agent=SoftwareAgent(
                name="renku test", id="https://github.com/swissdatasciencecenter/renku-python/tree/test"
            ),
        ),
        generations=[create_generation(g) for g in generations],
        usages=[create_usage(u) for u in usages],
        parameters=[
            ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=parameters_ids[name], value=value)
            for name, value in parameters.items()
        ],
    )


def create_dummy_plan(
    name: str,
    *,
    command: Optional[str] = None,
    date_created: Optional[datetime] = None,
    description: Optional[str] = None,
    index: Optional[int] = None,
    inputs: Iterable[Union[str, Tuple[str, str]]] = (),
    keywords: Optional[List[str]] = None,
    outputs: Iterable[Union[str, Tuple[str, str]]] = (),
    parameters: Iterable[Tuple[str, Any, Optional[str]]] = (),
    success_codes: Optional[List[int]] = None,
) -> "Plan":
    """Create a dummy plan."""
    from renku.domain_model.workflow.parameter import CommandInput, CommandOutput, CommandParameter, MappedIOStream
    from renku.domain_model.workflow.plan import Plan

    command = command or name

    id = Plan.generate_id(uuid=None if index is None else str(index))

    plan = Plan(
        command=command,
        date_created=date_created or local_now(),
        description=description,
        id=id,
        inputs=[],
        keywords=keywords,
        name=name,
        outputs=[],
        parameters=[],
        project_id="/projects/renku/abc123",
        success_codes=success_codes,
    )

    position = 1

    for index, parameter in enumerate(parameters, start=1):
        name, value, prefix = parameter
        plan.parameters.append(
            CommandParameter(
                default_value=value,
                id=CommandParameter.generate_id(plan_id=id, position=index),
                name=name,
                position=position,
                prefix=prefix,
            )
        )
        position += 1

    for index, input in enumerate(inputs, start=1):
        path, stream_type = input if isinstance(input, tuple) else (input, None)
        plan.inputs.append(
            CommandInput(
                default_value=path,
                id=CommandInput.generate_id(plan_id=id, position=index),
                mapped_to=MappedIOStream(stream_type=stream_type) if stream_type else None,
                name=f"input-{index}",
                position=position,
            )
        )
        position += 1

    for index, output in enumerate(outputs, start=1):
        path, stream_type = output if isinstance(output, tuple) else (output, None)
        plan.outputs.append(
            CommandOutput(
                default_value=path,
                id=CommandOutput.generate_id(plan_id=id, position=index),
                mapped_to=MappedIOStream(stream_type=stream_type) if stream_type else None,
                name=f"output-{index}",
                position=position,
            )
        )
        position += 1

    return plan


def clone_compressed_repository(base_path, name) -> "Repository":
    """Decompress and clone a repository."""
    import tarfile

    from renku.infrastructure.repository import Repository

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

    assert with_key in response.json.keys(), str(response.json)


def validate_cwl(cwl: Path):
    """Load a CWL file and raise an exception if it is invalid."""
    context = LoadingContext({"construct_tool_object": default_make_tool, "resolver": tool_resolver})
    load_tool(cwl.resolve().as_uri(), context)
