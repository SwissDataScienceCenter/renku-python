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
"""Models to represent a workflow definition file."""

from __future__ import annotations

import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Sequence, Union

import bashlex
import networkx as nx
from bashlex.ast import nodevisitor
from bashlex.errors import ParsingError
from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.plugin.workflow_file_parser import read_workflow_file
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.os import get_absolute_path, is_subpath
from renku.core.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.execute import execute_workflow_graph
from renku.core.workflow.plan import get_latest_plan, is_plan_removed
from renku.core.workflow.run import get_valid_parameter_name, get_valid_plan_name
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    MappedIOStream,
    WorkflowFileInput,
    generate_parameter_name,
)
from renku.domain_model.workflow.plan import get_duplicate_arguments_names
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan, WorkflowFilePlan


class WorkflowFile:
    """A workflow definition file."""

    def __init__(
        self,
        path: Union[Path, str],
        steps: List[Step],
        name: str,
        description: Optional[str] = None,
        keywords: Optional[List[str]] = None,
    ):
        self.path: str = str(path)
        self.steps: List[Step] = steps
        self.name: str = name
        self.description: Optional[str] = description
        self.keywords: List[str] = [str(k) for k in keywords] if keywords else []

        self.qualified_name = generate_qualified_plan_name(path=path, name=name)

        validate_workflow_file(self)
        set_missing_names(self)

    @inject.autoparams("project_gateway")
    def to_plan(self, project_gateway: IProjectGateway) -> WorkflowFileCompositePlan:
        """Convert a workflow file to a CompositePlan."""
        project_id = project_gateway.get_project().id

        workflow_file_root_plan = WorkflowFileCompositePlan(
            date_created=None,
            derived_from=None,
            description=self.description,
            id=WorkflowFileCompositePlan.generate_id(path=self.path),
            keywords=self.keywords.copy(),
            name=self.qualified_name,
            path=self.path,
            plans=[s.to_plan(project_id=project_id) for s in self.steps],
            project_id=project_id,
            qualified_name=self.qualified_name,
        )

        # NOTE: Make derivative chains and potentially re-use existing plans
        return calculate_derivatives(workflow_file_root_plan)


class Step:
    """A single step in a workflow file."""

    def __init__(
        self,
        *,
        command: str,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        inputs: List[Input] = None,
        keywords: Optional[List[str]] = None,
        name: str,
        original_command: Optional[str] = None,
        outputs: List[Output] = None,
        parameters: List[Parameter] = None,
        path: Union[Path, str],
        success_codes: Optional[List[int]] = None,
    ):
        self.command: str = command
        self.date_created = date_created or local_now()
        self.description: Optional[str] = description
        self.inputs: List[Input] = inputs or []
        self.keywords: List[str] = [str(k) for k in keywords] if keywords else []
        self.name: str = name
        # NOTE: Keep a copy of command since we will modify it during processing
        self.original_command = command if original_command is None else original_command
        self.outputs: List[Output] = outputs or []
        self.parameters: List[Parameter] = parameters or []
        self.path: str = str(path)
        self.success_codes: List[int] = [int(s) for s in success_codes] if success_codes else []

        self.qualified_name = generate_qualified_plan_name(path=path, name=name)

        StepCommandParser(step=self).parse_command()

    def to_plan(self, project_id: str) -> WorkflowFilePlan:
        """Convert a step to a WorkflowFilePlan."""
        id = WorkflowFilePlan.generate_id(path=self.path, name=self.name)
        inputs = [p.to_command_input(plan_id=id, index=i) for i, p in enumerate(self.inputs, start=1)]
        # NOTE: Add the workflow file itself as a dependency so that the workflow update machinery works
        workflow_file = WorkflowFileInput(
            default_value=self.path,
            description="Workflow file",
            id=CommandInput.generate_id(plan_id=id, name="workflow-file", postfix="workflow-file"),
            mapped_to=None,
            name="workflow-file",
            position=None,
            postfix="workflow-file",
            prefix=None,
        )
        inputs.append(workflow_file)

        return WorkflowFilePlan(
            command=self.command,
            date_created=self.date_created,
            derived_from=None,  # NOTE: We create the derivatives chain later
            description=self.description,
            id=id,
            inputs=inputs,
            keywords=self.keywords.copy(),
            name=self.qualified_name,
            outputs=[p.to_command_output(plan_id=id, index=i) for i, p in enumerate(self.outputs, start=1)],
            parameters=[p.to_command_parameter(plan_id=id, index=i) for i, p in enumerate(self.parameters, start=1)],
            path=self.path,
            project_id=project_id,
            qualified_name=self.qualified_name,
            success_codes=self.success_codes.copy(),
        )

    def __repr__(self) -> str:
        return f"<Step {self.name}: {self.command}>"


class BaseParameter:
    """Base class for Input, Output, and Parameter."""

    def __init__(
        self,
        description: Optional[str] = None,
        implicit: bool = False,
        name: Optional[str] = None,
        name_set_by_user: Optional[bool] = None,
        position: Optional[int] = None,
        prefix: Optional[str] = None,
    ):
        self.description: Optional[str] = description
        self.implicit: bool = implicit
        self.name: Optional[str] = name
        self.name_set_by_user: bool = bool(name) if name_set_by_user is None else name_set_by_user
        self.position: Optional[int] = position
        self.prefix: Optional[str] = prefix


class Parameter(BaseParameter):
    """A parameter for a workflow file."""

    def __init__(self, value: Any, **kwargs):
        super().__init__(**kwargs)
        self.value: Any = value

    def to_command_parameter(self, plan_id: str, index: int) -> CommandParameter:
        """Convert to a Plan parameter."""
        postfix = self.name if self.name_set_by_user else str(index)

        return CommandParameter(
            default_value=self.value,
            derived_from=None,
            description=self.description,
            id=CommandParameter.generate_id(plan_id=plan_id, name=self.name, postfix=postfix),
            name=self.name,
            position=self.position,
            postfix=postfix,
            prefix=self.prefix,
        )


class HiddenParameter(Parameter):
    """A parameter that isn't defined by user and is created by Renku."""


class BasePath(BaseParameter):
    """Base for workflow Input/Output."""

    def __init__(self, path: str, mapped_to: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.mapped_to: Optional[str] = mapped_to.lower().strip() if mapped_to else None
        self.path: str = path


class Input(BasePath):
    """An input to a workflow file."""

    def to_command_input(self, plan_id: str, index: int) -> CommandInput:
        """Convert to a Plan input."""
        postfix = self.name if self.name_set_by_user else str(index)
        return CommandInput(
            default_value=self.path,
            derived_from=None,
            description=self.description,
            encoding_format=None,
            id=CommandInput.generate_id(plan_id=plan_id, name=self.name, postfix=postfix),
            mapped_to=MappedIOStream.from_str(self.mapped_to) if self.mapped_to else None,
            name=self.name,
            position=self.position,
            postfix=postfix,
            prefix=self.prefix,
        )


class Output(BasePath):
    """An output from a workflow file."""

    def __init__(self, persist: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.persist: bool = persist

    def to_command_output(self, plan_id: str, index: int) -> CommandOutput:
        """Convert to a Plan output."""
        postfix = self.name if self.name_set_by_user else str(index)

        return CommandOutput(
            create_folder=False,
            default_value=self.path,
            derived_from=None,
            description=self.description,
            encoding_format=None,
            id=CommandOutput.generate_id(plan_id=plan_id, name=self.name, postfix=postfix),
            mapped_to=MappedIOStream.from_str(self.mapped_to) if self.mapped_to else None,
            name=self.name,
            position=self.position,
            postfix=postfix,
            prefix=self.prefix,
        )


def generate_qualified_plan_name(path: Union[Path, str], name: str) -> str:
    """Generate name for ``WorkflowFile`` and ``Step``."""
    # NOTE: Call ``as_posix()`` to convert Windows paths to posix paths to have a platform-independent name
    return f"{Path(path).as_posix()}::{name}"


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def run_workflow_file(
    path: Union[Path, str], steps: List[str], dry_run: bool, workflow_file: Optional[WorkflowFile], provider: str
):
    """Run a workflow file."""

    def find_step(plan):
        return next(s for s in workflow_file.steps if plan.qualified_name == s.qualified_name)

    from renku.command.view_model.workflow_file import WorkflowFileViewModel

    try:
        if not dry_run and not is_subpath(path=path, base=project_context.path):
            raise errors.ParameterError("Workflow file must be inside the project for execution")
    except errors.ProjectContextError:
        pass

    workflow_file = workflow_file or read_workflow_file(path=path, parser="renku")
    workflow = workflow_file.to_plan()

    # NOTE: Filter steps after calculating derivatives so that the root plan (that has a subset of steps) isn't
    # considered as a new version of the plan.
    selected_steps = filter_steps(workflow=workflow, steps=steps) if steps else [workflow]

    graph = ExecutionGraph(workflows=selected_steps, virtual_links=True)

    if not dry_run:
        # NOTE: We pass the non-filtered plan to be tracked; we can detect that a subset of steps were executed by
        # looking at the number of generated activities.
        execute_workflow_graph(dag=graph.workflow_graph, provider=provider, workflow_file_plan=workflow)
        dry_run_executions = []
    else:
        executed_steps = [find_step(plan) for plan in nx.topological_sort(graph.workflow_graph)]
        dry_run_executions = [f"Will execute step '{s.name}': {s.original_command}" for s in executed_steps]

    selected_steps_names = [s.name for s in selected_steps]
    workflow_file.steps = [s for s in workflow_file.steps if s.name in selected_steps_names]

    return WorkflowFileViewModel.from_workflow_file(workflow_file), dry_run_executions


def filter_steps(workflow: WorkflowFileCompositePlan, steps: List[str]) -> List[WorkflowFilePlan]:
    """Return a subset of workflow file steps."""
    selected_steps = set(steps)
    if len(steps) != len(selected_steps):
        communication.warn("Duplicated steps will be included only once.")

    not_found = selected_steps - {s.unqualified_name for s in workflow.plans}
    if not_found:
        not_found_str = ", ".join(not_found)
        raise errors.ParameterError(f"Cannot find steps: {not_found_str}")

    return [s for s in workflow.plans if s.unqualified_name in selected_steps]


def to_string(value: Any) -> str:
    """Convert a given value to string and strip surrounding whitespaces."""
    from renku.core.util.util import to_string

    return to_string(value).strip()


def validate_workflow_file(workflow_file: WorkflowFile):
    """Validate a workflow file."""

    def validate_plan_name(name, kind="Workflow file"):
        valid_name = get_valid_plan_name(name)
        if valid_name != name:
            raise errors.ParseError(f"{kind} name is invalid: '{name}' (Hint: '{valid_name}' is valid)")

    def is_name_reserved(name) -> bool:
        return name in ["inputs", "outputs", "parameters"]

    def validate_step(step):
        validate_plan_name(name=step.name, kind="Step")

        for parameter in itertools.chain(step.inputs, step.outputs, step.parameters):
            if parameter.name:
                valid_name = get_valid_parameter_name(parameter.name)
                if valid_name != parameter.name:
                    raise errors.ParseError(
                        f"Parameter name in step '{step.name}' is invalid: '{parameter.name}' "
                        f"(Hint: '{valid_name}' is valid)"
                    )

                if is_name_reserved(parameter.name):
                    raise errors.ParseError(
                        "Names 'inputs', 'outputs', and 'parameters' are reserved and cannot be used as parameter name "
                        f"in step '{step.name}'"
                    )

        duplicates = get_duplicate_arguments_names(plan=step)
        if duplicates:
            duplicates_string = ", ".join(sorted(duplicates))
            raise errors.ParseError(f"Duplicate input, output or parameter names found: {duplicates_string}")

    validate_plan_name(workflow_file.name)

    for s in workflow_file.steps:
        if s.name == workflow_file.name:
            raise errors.ParseError(f"Step '{s.name}' cannot have the same name as the workflow file")
        validate_step(s)


def set_missing_names(workflow_file: WorkflowFile):
    """Set missing names for attributes."""

    def is_name_unique(name, step) -> bool:
        for parameter in itertools.chain(step.inputs, step.outputs, step.parameters):
            if parameter.name == name:
                return False
        return True

    def is_name_reserved(name) -> bool:
        return name in ["inputs", "outputs", "parameters"]

    for step in workflow_file.steps:
        for parameter in itertools.chain(step.inputs, step.outputs, step.parameters):
            if not parameter.name:
                # NOTE: Use prefix as the name if it's unique
                if parameter.prefix:
                    valid_name = get_valid_parameter_name(parameter.prefix.strip(" -="))
                    if is_name_unique(name=valid_name, step=step) and not is_name_reserved(valid_name):
                        parameter.name = valid_name
                        continue

                parameter.name = generate_parameter_name(parameter=parameter, kind=parameter.__class__.__name__.lower())


@inject.autoparams("plan_gateway")
def calculate_derivatives(
    plan: Union[WorkflowFileCompositePlan, WorkflowFilePlan], plan_gateway: IPlanGateway, sequence: Optional[int] = None
) -> Union[WorkflowFileCompositePlan, WorkflowFilePlan]:
    """Check for existing plans and set derivative chain if needed.

    Args:
        plan(Union[WorkflowFileCompositePlan, WorkflowFilePlan]): The potential derivative plan.
        plan_gateway(IPlanGateway): ``PlanGateway`` instance.
        sequence(Optional[int]): sequence is used to generate deterministic IDs in case a previous ID cannot be used for
            a plan (e.g. when a plan was deleted and re-executed) (Default value = False).

    Returns:
        Union[WorkflowFileCompositePlan, WorkflowFilePlan]: An existing plan, if found and is the same; otherwise, the
            passed-in ``plan``.
    """

    def get_next_sequence() -> int:
        return 1 if sequence is None else sequence + 1

    existing_plan = plan_gateway.get_by_id(plan.id)
    existing_plan = get_latest_plan(plan=existing_plan)

    if not existing_plan:
        # NOTE: We need to check sub-plans
        assign_new_id = False
        create_derivatives = False
    elif is_plan_removed(existing_plan):
        # NOTE: The plan was deleted. Re-call with the next sequence ID
        sequence = get_next_sequence()
        plan.assign_new_id(sequence=sequence)
        return calculate_derivatives(plan=plan, sequence=sequence)
    elif not isinstance(existing_plan, type(plan)):
        # NOTE: A plan with the same ID existing that is not part of a workflow file, so, we cannot form a derivation
        # chain. Re-call with the next sequence ID.
        # should be improbable. Just change the ID. This also makes it impossible to ever form a derivation chain.
        sequence = get_next_sequence()
        plan.assign_new_id(sequence=sequence)
        return calculate_derivatives(plan=plan, sequence=sequence)
    elif plan.is_equal_to(existing_plan):  # type: ignore
        # NOTE: It's the same plan, so, re-use the existing one (which also re-uses sub-plans if any).
        return existing_plan  # type: ignore
    else:
        assign_new_id = True
        create_derivatives = True

    if assign_new_id:
        # NOTE: No need to pass ``sequence`` here since this ID doesn't need to be stable
        plan.assign_new_id()

    if create_derivatives:
        assert existing_plan  # NOTE: To make mypy happy!
        plan.derived_from = existing_plan.id

    if isinstance(plan, WorkflowFileCompositePlan):
        # NOTE: Chain or re-use sub-plans
        steps = []
        for step in plan.plans:
            potential_existing_step = calculate_derivatives(plan=step)
            steps.append(potential_existing_step)

        plan.plans = steps

    return plan


class StepCommandParser(nodevisitor):
    """A class to parse a workflow file step's command and figure out position of its arguments."""

    def __init__(self, step: Step):
        self.step: Step = step
        self.processed_parameters: List[Union[BasePath, BaseParameter]] = []

    def parse_command(self) -> None:
        """Parse the command and assign some properties (e.g. position) for parameters and the command."""
        self.processed_parameters = []

        try:
            # NOTE: Parse the command to an AST
            parsed_commands = bashlex.parse(self.step.command)
        except ParsingError as e:
            raise errors.ParseError(f"Cannot parse command {self.step.command}") from e

        if len(parsed_commands) > 1:
            raise errors.ParseError(f"Command must be a single line: {self.step.command}")

        parsed_command = parsed_commands[0]

        # NOTE: Process each token in the AST
        self.visit(parsed_command)

        unprocessed_nodes = [str(n) for n in parsed_command.parts if not n.processed]
        if unprocessed_nodes:
            nodes_str = "\n\t".join(unprocessed_nodes)
            communication.warn(f"The following command tokens weren't processed:\n\t{nodes_str}")

        parameters_with_position = (
            p
            for p in itertools.chain(self.step.inputs, self.step.outputs, self.step.parameters)
            if p.position is not None
        )
        parameters = sorted(parameters_with_position, key=lambda p: p.position)  # type: ignore
        for index, parameter in enumerate(parameters, start=1):
            parameter.position = index

        unprocessed_parameters = [
            p.name or p.prefix or str(getattr(p, "value", "")) or str(getattr(p, "path", ""))
            for p in itertools.chain(self.step.inputs, self.step.outputs, self.step.parameters)
            if not p.implicit and p not in self.processed_parameters
        ]
        if unprocessed_parameters:
            parameters_str = "\n\t".join(unprocessed_parameters)  # type: ignore
            communication.warn(
                f"The following inputs/outputs/parameters didn't appear in the command '{self.step.command}':"
                "\n(set 'implicit' attribute for them to 'true' if this is intentional)"
                f"\n\t{parameters_str}"
            )

    def visitnode(self, node):
        """Start of processing a single command token."""
        if node.kind in ["list", "pipeline"]:
            raise errors.ParseError(f"Cannot run multiple commands/piping in '{self.step.command}'")
        if node.kind not in ("command", "word", "redirect", "tilde", "parameter"):
            raise errors.ParseError(f"Unsupported command token: '{node}' in '{self.step.command}'")

        # NOTE: Set a marker for each node if not already set
        if getattr(node, "processed", None) is None:
            node.processed = False

    def visitcommand(self, node, parts):
        """Process a full command.

        NOTE: There is exactly one command node since we don't allow piping or multiple commands. Therefore, this method
        is called only once.
        """
        if not parts:
            raise errors.ParseError(f"No command to parse: {node}")

        node.processed = True

        command: str = ""
        command_done: bool = False
        prefix: str = ""
        position_offset = 0

        def get_position(node, increase_offset: bool = False) -> int:
            nonlocal position_offset

            value = position_offset + node.pos[0]
            if increase_offset:
                position_offset += 1

            return value

        def process_variable(token: str, node) -> bool:
            """If token is a variable then set position for its parameter(s) and return True."""
            # NOTE: Check for $$ which is used to escape $
            if token.startswith("$$") or not token.startswith("$"):
                return False

            name = token.replace("$", "", 1)
            name = self.unescape_dollar_sign(name)
            parameter: BaseParameter

            if name == "parameters":
                for parameter in self.step.parameters:
                    parameter.position = get_position(node, increase_offset=True)
                    self.processed_parameters.append(parameter)
            elif name == "inputs":
                for parameter in self.step.inputs:
                    parameter.position = get_position(node, increase_offset=True)
                    self.processed_parameters.append(parameter)
            elif name == "outputs":
                for parameter in self.step.outputs:
                    parameter.position = get_position(node, increase_offset=True)
                    self.processed_parameters.append(parameter)
            else:
                argument = self.find_argument_by_name(name=name)
                if not argument:
                    raise errors.ParseError(f"Cannot find a parameter with name '{name}'")
                argument.position = get_position(node)
                self.processed_parameters.append(argument)

            return True

        for index, node in enumerate(parts):
            # NOTE: We assume all redirections come at the end of the command. So, stop processing once reaching a
            # ``redirect`` node. Redirects are processed separately.
            if node.kind == "redirect":
                break

            if node.kind == "word":
                node.processed = True
                word = node.word
                assert isinstance(word, str), f"Token value isn't string: {node}"

                if process_variable(token=word, node=node):
                    continue

                word = self.unescape_dollar_sign(word)

                # NOTE: If no command has not been found yet, don't search in the outputs
                argument = self.find_argument(argument=word, prefix=prefix, search_outputs=bool(command))
                if argument:
                    command_done = True
                    prefix = ""
                    argument.position = get_position(node)
                    self.processed_parameters.append(argument)
                elif word == "--":
                    if prefix:
                        raise errors.ParseError(f"Cannot find a parameter for '{prefix}'")
                    command_done = True
                    position = get_position(node)
                    parameter = HiddenParameter(name=f"hidden-{position}", value=word, position=position)
                    self.step.parameters.append(parameter)
                    self.processed_parameters.append(parameter)
                elif not command_done and command and not prefix:
                    # NOTE: If a command is already found, word might be a prefix
                    prefix = word
                elif not command_done and command and prefix:
                    # NOTE: Previous ``word`` wasn't a prefix, so, add it to the command
                    command = f"{command} {prefix}"
                    prefix = word
                elif not command_done:
                    command = word if not command else f"{command} {word}"
                elif prefix:
                    raise errors.ParseError(f"Cannot find a parameter for '{prefix} {word}'")
                else:
                    prefix = word
            elif node.kind == "tilde":
                # NOTE: ``tilde`` nodes appear when there is a path with ``~`` (e.g. ~/data.csv)
                node.processed = True

        if prefix:
            if not command_done:
                command = f"{command} {prefix}"
            else:
                raise errors.ParseError(f"Cannot find a parameter for '{prefix}'")

        self.step.command = command

    def visitredirect(self, node, input, type, output, heredoc):
        """Process a redirect."""

        def is_parameter(node):
            return (
                node.output
                and isinstance(getattr(node.output, "word", None), str)
                and node.output.word.startswith("$")
                and not node.output.word.startswith("$$")
            )

        if type in (">", ">|"):  # stdout or stderr
            if input is not None and (not isinstance(input, int) or input not in (1, 2)):
                raise errors.ParseError(f"Unsupported output redirection '{input}{type}' in {self.step.command}")

            stream = "stdout" if input is None or input == 1 else "stderr"  # input == 2

            if is_parameter(node):
                name = node.output.word.replace("$", "", 1)
                name = self.unescape_dollar_sign(name)
                if name == "outputs":
                    if len(self.step.outputs) != 1:
                        raise errors.ParseError(
                            f"There must be exactly one output when using '$outputs' as {stream} "
                            f"in step '{self.step.name}'"
                        )
                    parameter = self.step.outputs[0]
                else:
                    parameter = self.find_argument_by_name(name=name, collection=self.step.outputs)
                    if not parameter:
                        raise errors.ParseError(f"Cannot find variable '{name}' for {stream}")
            else:
                path = output.word
                parameter = self.find_output(path=path, prefix=None)
                if not parameter:
                    raise errors.ParseError(f"Cannot find '{stream}' == '{path}' in outputs for {self.step.command}")

            parameter.mapped_to = stream
        elif type == "<":  # stdin
            if is_parameter(node):
                name = node.output.word.replace("$", "", 1)
                name = self.unescape_dollar_sign(name)
                if name == "inputs":
                    if len(self.step.inputs) != 0:
                        raise errors.ParseError(
                            "There must be exactly one input when using '$inputs' in stdin"
                            f" in step '{self.step.name}'"
                        )
                    parameter = self.step.inputs[0]
                else:
                    parameter = self.find_argument_by_name(name=name, collection=self.step.inputs)
                    if not parameter:
                        raise errors.ParseError(f"Cannot find variable '{name}' for stdin")
            else:
                path = output.word
                parameter = self.find_input(path=path, prefix=None)
                if not parameter:
                    raise errors.ParseError(f"Cannot find 'stdin' == '{path}' in inputs for {self.step.command}")

            parameter.mapped_to = "stdin"
        elif type in (">&", "<&"):
            # NOTE: Renku metadata doesn't support a stream that is mapped to another
            stream = f"{to_string(input)}{type}{output}"
            raise errors.ParseError(f"Stream-to-stream redirection is not supported '{stream}' in {self.step.command}")
        elif type == ">>":
            stream = f"{to_string(input)}{type}{output}"
            raise errors.ParseError(f"Appending redirection is not supported '{stream}' in {self.step.command}")
        else:
            raise errors.ParseError(f"Unsupported redirection operator '{type}' in {self.step.command}")

        node.processed = True
        node.output.processed = True

        parameter.position = node.pos[0]
        self.processed_parameters.append(parameter)

    @staticmethod
    def unescape_dollar_sign(word: str) -> str:
        """Unescape $ in the command."""
        return word.replace("$$", "$")

    def find_argument(self, argument: str, prefix: Optional[str], search_outputs: bool) -> Optional[BaseParameter]:
        """Check the argument against inputs/outputs/parameters to find its type."""
        parameter = self.find_and_process_parameter(value=argument, prefix=prefix)
        if parameter:
            return parameter

        input = self.find_input(path=argument, prefix=prefix)
        if input:
            return input

        if search_outputs:
            output = self.find_output(path=argument, prefix=prefix)
            if output:
                return output

        return None

    def find_argument_by_name(self, name: str, collection=None) -> Optional[BaseParameter]:
        """Find an inputs/outputs/parameters with the given name."""
        collection = collection or itertools.chain(self.step.inputs, self.step.outputs, self.step.parameters)
        argument = next((p for p in collection if p.name == name), None)
        if argument:
            self.processed_parameters.append(argument)

        return argument

    def find_and_process_parameter(self, value: str, prefix: Optional[str]) -> Optional[Parameter]:
        """Find a parameter based on its value and prefix; adjust parameter's prefix if needed."""
        value = to_string(value)
        prefix = to_string(prefix)

        has_equal = False
        new_prefix, new_value = "", ""
        if "=" in value and not prefix:
            has_equal = True
            new_prefix, new_value = value.split("=", maxsplit=1)

        for parameter in self.step.parameters:
            # NOTE: Process each parameter only once
            if parameter in self.processed_parameters:
                continue

            if value == to_string(parameter.value) and prefix == to_string(parameter.prefix):
                if parameter.prefix:
                    # NOTE: Add a separator between prefix and value
                    parameter.prefix = parameter.prefix.strip() + " "
                return parameter
            # NOTE: If no prefix is given then it's possible that value is a parameter prefix
            elif not prefix and not parameter.prefix and value == to_string(parameter.prefix):
                return parameter
            elif (
                has_equal
                and new_prefix == to_string(parameter.prefix).strip("=")
                and new_value == to_string(parameter.value)
            ):
                if parameter.prefix:
                    # NOTE: Add proper separator between prefix and value
                    parameter.prefix = parameter.prefix.strip("=") + "="
                return parameter

        # NOTE: Do this in a second loop since the previous comparisons have higher priority
        for parameter in self.step.parameters:
            # NOTE: Process each parameter only once
            if parameter in self.processed_parameters:
                continue

            if not parameter.prefix or not parameter.value:
                combined_value = (
                    f"{prefix} {value}"
                    if isinstance(parameter.value, str) and " " in parameter.value
                    else f"{prefix}{value}"
                )
                parameter_value = parameter.prefix or parameter.value
                # NOTE: Use ``split`` to ignore multiple whitespaces in comparison
                if combined_value.split() == str(parameter_value).split():
                    return parameter

        return None

    def find_input(self, path: str, prefix: Optional[str]) -> Optional[BasePath]:
        """Search for a given path in inputs."""
        return self.find_and_process_path(collection=self.step.inputs, path=path, prefix=prefix)

    def find_output(self, path: str, prefix: Optional[str]) -> Optional[BasePath]:
        """Search for a given path in outputs."""
        return self.find_and_process_path(collection=self.step.outputs, path=path, prefix=prefix)

    def find_and_process_path(
        self, collection: Sequence[BasePath], path: str, prefix: Optional[str]
    ) -> Optional[BasePath]:
        """Find the given path in the collection."""

        def are_paths_equal(a: Union[Path, str], b: Union[Path, str]) -> bool:
            """Returns if two paths are the same."""
            # NOTE: The two paths should be identical; we don't consider the case where one is a sub-path of another
            return get_absolute_path(a) == get_absolute_path(b)

        prefix = to_string(prefix).strip("=")

        for parameter in collection:
            if parameter in self.processed_parameters:
                continue

            if are_paths_equal(path, parameter.path) and prefix == to_string(parameter.prefix).strip("="):
                if parameter.prefix and not parameter.prefix.endswith("="):
                    # NOTE: Add a separator between prefix and value
                    parameter.prefix = parameter.prefix.strip() + " "
                return parameter

        return None


def get_workflow_file_generations(workflow_file: WorkflowFile) -> List[str]:
    """Return a list of all outputs that must be committed."""
    return [o.path for step in workflow_file.steps for o in step.outputs if o.persist]
