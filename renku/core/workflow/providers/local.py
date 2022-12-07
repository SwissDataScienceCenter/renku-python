# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""local machine executor provider."""

import contextlib
import itertools
import os
import subprocess
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator

import networkx as nx

from renku.core import errors
from renku.core.plugin import hookimpl
from renku.core.plugin.provider import RENKU_ENV_PREFIX
from renku.core.util import communication
from renku.domain_model.workflow.provider import IWorkflowProvider

if TYPE_CHECKING:
    from renku.domain_model.workflow.plan import Plan


class LocalWorkflowProvider(IWorkflowProvider):
    """A workflow executor provider on the local machine."""

    @hookimpl
    def workflow_provider(self):
        """Workflow provider name."""
        return self, "local"

    @hookimpl
    def workflow_execute(self, dag: nx.DiGraph, basedir: Path, config: Dict[str, Any]):
        """Executes a given workflow."""
        for plan in nx.topological_sort(dag):
            execute_plan(plan)


def execute_plan(plan: "Plan"):
    """Execute a plan on the local machine."""
    # NOTE: Quoting string values causes a double quoting when passed to ``subprocess.run``
    command_line = plan.to_argv(quote_string=False)

    os_env = os.environ.copy()
    env = get_workflow_parameters_env_vars(workflow=plan)
    os_env.update(env)

    try:
        command_str = " ".join(plan.to_argv(with_streams=True))
        communication.echo(f"Executing step '{plan.name}': '{command_str}' ...")
        with get_plan_std_stream_mapping(plan) as std_streams_mappings:
            return_code = subprocess.run(command_line, cwd=os.getcwd(), env=os_env, **std_streams_mappings).returncode
    except OSError:
        tb = "\n  ".join(traceback.format_exc().split("\n"))
        raise errors.WorkflowExecuteError(f"Execution of step '{plan.name}' failed:\n\n  {tb}", show_prefix=False)

    success_codes = plan.success_codes or [0]
    if return_code not in success_codes:
        message = f"Execution of step '{plan.name}' returned {return_code} exit status which is not in {success_codes}"
        raise errors.InvalidSuccessCode(return_code=return_code, message=message)


@contextlib.contextmanager
def get_plan_std_stream_mapping(plan: "Plan") -> Generator[Dict[str, Any], None, None]:
    """Return the mapping between std streams and file paths for a plan."""
    arguments = itertools.chain(plan.inputs, plan.outputs)
    std_arguments = [a for a in arguments if a.mapped_to]  # type: ignore

    for argument in std_arguments:
        Path(str(argument.actual_value)).parent.mkdir(parents=True, exist_ok=True)

    open_mode = {"stdin": "r", "stdout": "w", "stderr": "w"}
    streams = {}

    with contextlib.ExitStack() as stack:
        for a in std_arguments:
            file = stack.enter_context(open(str(a.actual_value), open_mode[a.mapped_to.stream_type]))  # type: ignore
            streams[a.mapped_to.stream_type] = file  # type: ignore

        yield streams


def get_workflow_parameters_env_vars(workflow: "Plan") -> Dict[str, str]:
    """Get env vars for all workflow inputs/outputs/parameters."""
    env = {}

    for parameter in itertools.chain(workflow.inputs, workflow.parameters):
        env[f"{RENKU_ENV_PREFIX}{parameter.name}"] = str(parameter.actual_value)

    for o in workflow.outputs:
        env[f"{RENKU_ENV_PREFIX}{o.name}"] = str(o.actual_value)
        output_path = Path(o.actual_value)
        if len(output_path.parts) > 1:
            output_path.parent.mkdir(parents=True, exist_ok=True)

    return env
