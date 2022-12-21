# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Running workflows logic."""

import os
import sys
from collections import defaultdict
from pathlib import Path
from subprocess import call
from typing import Dict, List, NamedTuple, Optional, Set, Union, cast

import click
from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.command.view_model.plan import PlanViewModel
from renku.core import errors
from renku.core.config import get_value
from renku.core.git import get_mapped_std_streams
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.storage import check_external_storage, pull_paths_from_storage
from renku.core.util.datetime8601 import local_now
from renku.core.util.git import get_git_user
from renku.core.util.os import get_relative_path_to_cwd, get_relative_paths
from renku.core.util.urls import get_slug
from renku.core.workflow.activity import (
    get_all_modified_and_deleted_activities_and_entities,
    get_downstream_generating_activities,
    is_activity_valid,
)
from renku.core.workflow.plan_factory import PlanFactory
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.activity import Activity
from renku.domain_model.provenance.agent import Person


class StatusResult(NamedTuple):
    """Represent status of a project.

    A quintuple containing a mapping of stale outputs to modified usages, a mapping of stale activities that have no
    generation to modified usages, a set of modified usages, a set of deleted usages, and a map of modified hidden
    inputs to paths that they generate.
    """

    outdated_outputs: Dict[str, Set[str]]
    outdated_activities: Dict[str, Set[str]]
    modified_inputs: Set[str]
    deleted_inputs: Set[str]
    modified_hidden_inputs: Dict[str, Set[str]]


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def get_status(paths: Optional[List[Union[Path, str]]] = None, ignore_deleted: bool = False) -> StatusResult:
    """Return status of a project.

    Args:
        paths(Optional[List[Union[Path, str]]]): Limit the status to this list of paths (Default value = None).
        ignore_deleted(bool): Whether to ignore deleted generations (Default value = False).

    Returns:
        StatusResult: Status of the project.

    """

    def get_all_generations(activity):
        return (
            get_relative_path_to_cwd(project_context.path / generation.entity.path)
            for generation in activity.generations
        )

    def get_downstream_activities(start_activity, entity, ignore_deleted):
        # NOTE: Add all downstream activities if the modified entity is in paths; otherwise, add only activities that
        # chain-generate at least one of the paths
        generation_paths = [] if not paths or entity.path in paths else paths

        return get_downstream_generating_activities(
            starting_activities={start_activity},
            paths=generation_paths,
            ignore_deleted=ignore_deleted,
            project_path=project_context.path,
        )

    ignore_deleted = ignore_deleted or get_value("renku", "update_ignore_delete")

    modified, deleted, hidden_modified = get_all_modified_and_deleted_activities_and_entities(
        repository=project_context.repository, check_hidden_dependencies=True
    )

    modified = {(a, e) for a, e in modified if is_activity_valid(a)}
    deleted = {(a, e) for a, e in deleted if is_activity_valid(a)}
    hidden_modified = {(a, e) for a, e in hidden_modified if is_activity_valid(a)}

    if not modified and not deleted and not hidden_modified:
        return StatusResult({}, {}, set(), set(), {})

    paths = paths or []
    paths = get_relative_paths(base=project_context.path, paths=[Path.cwd() / p for p in paths])  # type: ignore

    modified_inputs: Set[str] = set()
    stale_outputs: Dict[str, Set[str]] = defaultdict(set)
    stale_activities: Dict[str, Set[str]] = defaultdict(set)
    modified_hidden_inputs: Dict[str, Set[str]] = defaultdict(set)

    for start_activity, entity in modified:
        activities = get_downstream_activities(
            start_activity=start_activity, entity=entity, ignore_deleted=ignore_deleted
        )
        if activities:
            usage_path = get_relative_path_to_cwd(project_context.path / entity.path)
            modified_inputs.add(usage_path)

            for activity in activities:
                if len(activity.generations) == 0:
                    stale_activities[activity.id].add(usage_path)
                else:
                    for generation_path in get_all_generations(activity):
                        stale_outputs[generation_path].add(usage_path)

    deleted_paths = {e.path for _, e in deleted}
    deleted_paths = {
        get_relative_path_to_cwd(project_context.path / d) for d in deleted_paths if not paths or d in paths
    }

    for start_activity, entity in hidden_modified:
        activities = get_downstream_activities(start_activity=start_activity, entity=entity, ignore_deleted=True)
        if activities:
            usage_path = get_relative_path_to_cwd(project_context.path / entity.path)
            for activity in activities:
                for generation_path in get_all_generations(activity):
                    modified_hidden_inputs[usage_path].add(generation_path)

    return StatusResult(
        outdated_outputs=stale_outputs,
        outdated_activities=stale_activities,
        modified_inputs=modified_inputs,
        deleted_inputs=deleted_paths,
        modified_hidden_inputs=modified_hidden_inputs,
    )


def get_valid_plan_name(name: str) -> str:
    """Return a valid plan name based on the passed name."""
    return get_slug(name, invalid_chars=["."], lowercase=False)


def get_valid_parameter_name(name: str) -> str:
    """Return a valid parameter name based on the passed name."""
    return get_valid_plan_name(name)


@inject.autoparams("activity_gateway", "plan_gateway")
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def run_command_line(
    name,
    description,
    keyword,
    explicit_inputs,
    explicit_outputs,
    explicit_parameters,
    no_output,
    no_input_detection,
    no_output_detection,
    success_codes,
    command_line,
    creators,
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
) -> PlanViewModel:
    """Run command line arguments."""
    # NOTE: validate name as early as possible
    if name:
        valid_name = get_valid_plan_name(name)
        if name != valid_name:
            raise errors.ParameterError(f"Invalid name: '{name}' (Hint: '{valid_name}' is valid).")

        workflows = plan_gateway.get_newest_plans_by_names()
        if name in workflows:
            raise errors.DuplicateWorkflowNameError(f"Duplicate workflow name: Workflow '{name}' already exists.")

    paths = explicit_outputs if no_output_detection else project_context.repository.all_files
    mapped_std = get_mapped_std_streams(paths, streams=("stdout", "stderr"))

    paths = explicit_inputs if no_input_detection else project_context.repository.all_files
    mapped_std_in = get_mapped_std_streams(paths, streams=("stdin",))
    mapped_std.update(mapped_std_in)

    invalid = get_mapped_std_streams(explicit_inputs, streams=("stdout", "stderr"))
    if invalid:
        raise errors.UsageError(
            "Explicit input file cannot be used as stdout/stderr:"
            "\n\t" + click.style("\n\t".join(invalid.values()), fg="yellow") + "\n"
        )

    invalid = get_mapped_std_streams(explicit_outputs, streams=("stdin",))
    if invalid:
        raise errors.UsageError(
            "Explicit output file cannot be used as stdin:"
            "\n\t" + click.style("\n\t".join(invalid.values()), fg="yellow") + "\n"
        )

    system_stdout = None
    system_stderr = None

    # /dev/tty is a virtual device that points to the terminal
    # of the currently executed process
    try:
        with open("/dev/tty", "w"):
            tty_exists = True
    except OSError:
        tty_exists = False

    try:
        stdout_redirected = "stdout" in mapped_std
        stderr_redirected = "stderr" in mapped_std

        if tty_exists:
            # if renku was called with redirected stdout/stderr, undo the
            # redirection here so error messages can be printed normally
            if stdout_redirected:
                system_stdout = open("/dev/tty", "w")
                old_stdout = sys.stdout
                sys.stdout = system_stdout

            if stderr_redirected:
                system_stderr = open("/dev/tty", "w")
                old_stderr = sys.stderr
                sys.stderr = system_stderr

        working_dir = str(project_context.path)

        def parse_explicit_definition(entries, type):
            result = [tuple(e.split("=", maxsplit=1)[::-1]) if "=" in e else (e, None) for e in entries]

            if not result:
                return result

            values, names = zip(*result)

            if len(values) != len(set(values)):
                raise errors.UsageError(f"Cannot specify the same explicit {type} value twice.")

            names = [n for n in names if n]

            if names and len(names) != len(set(names)):
                raise errors.UsageError(f"Cannot specify the same explicit {type} name twice.")

            return result

        explicit_inputs = parse_explicit_definition(explicit_inputs, "input")
        explicit_outputs = parse_explicit_definition(explicit_outputs, "output")
        explicit_parameters = parse_explicit_definition(explicit_parameters, "param")

        factory = PlanFactory(
            command_line=command_line,
            explicit_inputs=explicit_inputs,
            explicit_outputs=explicit_outputs,
            explicit_parameters=explicit_parameters,
            directory=os.getcwd(),
            working_dir=working_dir,
            no_input_detection=no_input_detection,
            no_output_detection=no_output_detection,
            success_codes=success_codes,
            **{name: os.path.relpath(path, working_dir) for name, path in mapped_std.items()},
        )
        with factory.watch(no_output=no_output) as tool:
            # Don't compute paths if storage is disabled.
            if check_external_storage():
                # Make sure all inputs are pulled from a storage.
                paths_ = (path for _, path in tool.iter_input_files(project_context.path))
                pull_paths_from_storage(project_context.repository, *paths_)

            if tty_exists:
                # apply original output redirection
                if stdout_redirected:
                    sys.stdout = old_stdout
                if stderr_redirected:
                    sys.stderr = old_stderr

            started_at_time = local_now()

            try:
                return_code = call(
                    factory.command_line, cwd=os.getcwd(), **{key: getattr(sys, key) for key in mapped_std.keys()}
                )
            except FileNotFoundError:
                command = " ".join(factory.base_command)
                raise errors.ParameterError(
                    f"Cannot execute command '{command}': "
                    "This is likely because the executable doesn't exist or has the wrong permissions set."
                )

            ended_at_time = local_now()

            sys.stdout.flush()
            sys.stderr.flush()

            if tty_exists:
                # change back to /dev/tty redirection
                if stdout_redirected:
                    sys.stdout = system_stdout  # type:ignore
                if stderr_redirected:
                    sys.stderr = system_stderr  # type:ignore

            if return_code not in (success_codes or {0}):
                raise errors.InvalidSuccessCode(return_code, success_codes=success_codes)

        if not creators:
            creators = [cast(Person, get_git_user(project_context.repository))]

        plan = tool.to_plan(
            name=name, description=description, keywords=keyword, creators=creators, date_created=started_at_time
        )
        activity = Activity.from_plan(
            plan=plan,
            repository=project_context.repository,
            started_at_time=started_at_time,
            ended_at_time=ended_at_time,
            annotations=tool.annotations,
        )
        activity_gateway.add(activity)

        return PlanViewModel.from_plan(plan)

    finally:
        if system_stdout:
            system_stdout.flush()
            sys.stdout = old_stdout
            system_stdout.close()
        if system_stderr:
            system_stderr.flush()
            sys.stderr = old_stderr
            system_stderr.close()
