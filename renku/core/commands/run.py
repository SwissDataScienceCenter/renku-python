# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku run command."""

import os
import sys
from subprocess import call

import click

from renku.core import errors
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.git import get_mapped_std_streams
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.workflow.plan_factory import PlanFactory
from renku.core.models.provenance.activity import Activity
from renku.core.utils.datetime8601 import local_now
from renku.core.utils.urls import get_slug


def run_command():
    """Tracking work on a specific problem."""
    return Command().command(_run_command).require_migration().require_clean().with_database(write=True).with_commit()


@inject.autoparams()
def _run_command(
    name,
    description,
    keyword,
    explicit_inputs,
    explicit_outputs,
    no_output,
    no_input_detection,
    no_output_detection,
    success_codes,
    command_line,
    client_dispatcher: IClientDispatcher,
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
):
    # NOTE: validate name as early as possible
    client = client_dispatcher.current_client

    if name:
        valid_name = get_slug(name, invalid_chars=["."], lowercase=False)
        if name != valid_name:
            raise errors.ParameterError(f"Invalid name: '{name}' (Hint: '{valid_name}' is valid).")

        workflows = plan_gateway.get_newest_plans_by_names()
        if name in workflows:
            raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    paths = explicit_outputs if no_output_detection else client.candidate_paths
    mapped_std = get_mapped_std_streams(paths, streams=("stdout", "stderr"))

    paths = explicit_inputs if no_input_detection else client.candidate_paths
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

        working_dir = str(client.repository.path)
        factory = PlanFactory(
            command_line=command_line,
            explicit_inputs=explicit_inputs,
            explicit_outputs=explicit_outputs,
            directory=os.getcwd(),
            working_dir=working_dir,
            no_input_detection=no_input_detection,
            no_output_detection=no_output_detection,
            success_codes=success_codes,
            **{name: os.path.relpath(path, working_dir) for name, path in mapped_std.items()},
        )
        with factory.watch(no_output=no_output) as tool:
            # Don't compute paths if storage is disabled.
            if client.check_external_storage():
                # Make sure all inputs are pulled from a storage.
                paths_ = (path for _, path in tool.iter_input_files(client.path))
                client.pull_paths_from_storage(*paths_)

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
                raise errors.ParameterError(f"Cannot execute command '{command}'")

            ended_at_time = local_now()

            sys.stdout.flush()
            sys.stderr.flush()

            if tty_exists:
                # change back to /dev/tty redirection
                if stdout_redirected:
                    sys.stdout = system_stdout
                if stderr_redirected:
                    sys.stderr = system_stderr

            if return_code not in (success_codes or {0}):
                raise errors.InvalidSuccessCode(return_code, success_codes=success_codes)

        plan = tool.to_plan(name=name, description=description, keywords=keyword)
        activity = Activity.from_plan(
            plan=plan, started_at_time=started_at_time, ended_at_time=ended_at_time, annotations=tool.annotations
        )
        activity_gateway.add(activity)

    finally:
        if system_stdout:
            system_stdout.flush()
            sys.stdout = old_stdout
            system_stdout.close()
        if system_stderr:
            system_stderr.flush()
            sys.stderr = old_stderr
            system_stderr.close()
