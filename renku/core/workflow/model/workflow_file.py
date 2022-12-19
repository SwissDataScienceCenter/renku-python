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
from typing import Any, Callable, List, Optional, Sequence, Type, TypeVar, Union, cast, overload

import bashlex
from bashlex.ast import nodevisitor
from bashlex.errors import ParsingError

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.os import are_paths_equal
from renku.core.util.util import to_string
from renku.core.workflow.plan import get_latest_plan, is_plan_removed
from renku.core.workflow.run import get_valid_parameter_name, get_valid_plan_name
from renku.domain_model.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    HiddenInput,
    MappedIOStream,
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

        self.qualified_name = name

        self.validate()
        self.set_missing_names()

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
        )

        # NOTE: Make derivative chains and potentially re-use existing plans
        return calculate_derivatives(workflow_file_root_plan)

    def validate(self):
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
                            "Names 'inputs', 'outputs', and 'parameters' are reserved and cannot be used as parameter "
                            f"name in step '{step.name}'"
                        )

            duplicates = get_duplicate_arguments_names(plan=step)
            if duplicates:
                duplicates_string = ", ".join(sorted(duplicates))
                raise errors.ParseError(f"Duplicate input, output or parameter names found: {duplicates_string}")

        validate_plan_name(self.name)

        for s in self.steps:
            if s.name == self.name:
                raise errors.ParseError(f"Step '{s.name}' cannot have the same name as the workflow file")
            validate_step(s)

    def set_missing_names(self):
        """Set missing names for attributes."""

        def is_name_unique(name, step) -> bool:
            for parameter in itertools.chain(step.inputs, step.outputs, step.parameters):
                if parameter.name == name:
                    return False
            return True

        def is_name_reserved(name) -> bool:
            return name in ["inputs", "outputs", "parameters"]

        for step in self.steps:
            for parameter in itertools.chain(step.inputs, step.outputs, step.parameters):
                if not parameter.name:
                    # NOTE: Use prefix as the name if it's unique
                    if parameter.prefix:
                        valid_name = get_valid_parameter_name(parameter.prefix.strip(" -="))
                        if is_name_unique(name=valid_name, step=step) and not is_name_reserved(valid_name):
                            parameter.name = valid_name
                            continue

                    kind = parameter.__class__.__name__.lower()
                    parameter.name = generate_parameter_name(parameter=parameter, kind=kind)


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
        workflow_file_name: str,
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
        self.success_codes: List[int] = success_codes or []

        self.qualified_name = generate_qualified_plan_name(workflow_file_name=workflow_file_name, step_name=name)

        StepCommandParser(step=self).parse_command()

    def to_plan(self, project_id: str) -> WorkflowFilePlan:
        """Convert a step to a WorkflowFilePlan."""
        id = WorkflowFilePlan.generate_id(path=self.path, name=self.name)

        # NOTE: Add the workflow file itself as a hidden dependency
        workflow_file = HiddenInput(
            default_value=self.path,
            description="Workflow file",
            id=CommandInput.generate_id(plan_id=id, name="workflow-file", postfix="workflow-file"),
            mapped_to=None,
            name="workflow-file",
            position=None,
            postfix="workflow-file",
            prefix=None,
        )

        return WorkflowFilePlan(
            command=self.command,
            date_created=self.date_created,
            derived_from=None,  # NOTE: We create the derivatives chain later
            description=self.description,
            hidden_inputs=[workflow_file],
            id=id,
            inputs=[p.to_command_input(plan_id=id, index=i) for i, p in enumerate(self.inputs, start=1)],
            keywords=self.keywords.copy(),
            name=self.qualified_name,
            outputs=[p.to_command_output(plan_id=id, index=i) for i, p in enumerate(self.outputs, start=1)],
            parameters=[p.to_command_parameter(plan_id=id, index=i) for i, p in enumerate(self.parameters, start=1)],
            path=self.path,
            project_id=project_id,
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


BaseParameterType = TypeVar("BaseParameterType", bound=BaseParameter)


class Parameter(BaseParameter):
    """A parameter for a workflow file."""

    def __init__(self, value: Any, **kwargs):
        super().__init__(**kwargs)
        self.value: Any = value

        if not value:
            raise errors.ParameterError("Attribute 'value' must be set for 'Parameter' instances")

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

    def __init__(self, path: str, mapped_to: Optional[str] = None, persist: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.mapped_to: Optional[str] = mapped_to.lower().strip() if mapped_to else None
        self.path: str = path
        self.persist: bool = persist

        if not path:
            raise errors.ParameterError(f"Attribute 'path' must be set for '{self.__class__.__name__}' instances")


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

    def to_command_output(self, plan_id: str, index: int) -> CommandOutput:
        """Convert to a Plan output."""
        postfix = self.name if self.name_set_by_user else str(index)
        path = Path(self.path)
        create_folder = not path.exists() and path.parent.resolve() != Path.cwd()

        return CommandOutput(
            create_folder=create_folder,
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


def generate_qualified_plan_name(workflow_file_name: str, step_name: str) -> str:
    """Generate name for ``WorkflowFile`` and ``Step``."""
    # NOTE: Call ``as_posix()`` to convert Windows paths to posix paths to have a platform-independent name
    return f"{workflow_file_name}.{step_name}"


@inject.autoparams("plan_gateway")
def calculate_derivatives(
    plan: Union[WorkflowFileCompositePlan, WorkflowFilePlan],
    plan_gateway: IPlanGateway,
    sequence: Optional[itertools.count] = None,
) -> Union[WorkflowFileCompositePlan, WorkflowFilePlan]:
    """Check for existing plans and set derivative chain if needed.

    Args:
        plan(Union[WorkflowFileCompositePlan, WorkflowFilePlan]): The potential derivative plan.
        plan_gateway(IPlanGateway): ``PlanGateway`` instance.
        sequence(Optional[itertools.count]): sequence is used to generate deterministic IDs in case a previous ID
             cannot be used for a plan (e.g. when a plan was deleted and re-executed) (Default value = False).

    Returns:
        Union[WorkflowFileCompositePlan, WorkflowFilePlan]: An existing plan, if found and is the same; otherwise, the
            passed-in ``plan``.
    """

    def get_next_sequence_value() -> int:
        nonlocal sequence

        if not sequence:
            sequence = itertools.count(0)

        return next(sequence)

    existing_plan = plan_gateway.get_by_id(plan.id)
    existing_plan = get_latest_plan(plan=existing_plan)

    if not existing_plan:
        # NOTE: We need to check sub-plans
        assign_new_id = False
        create_derivatives = False
    elif is_plan_removed(existing_plan):
        # NOTE: The plan was deleted. Re-call with the next sequence ID
        sequence_value = get_next_sequence_value()
        plan.assign_new_id(sequence=sequence_value)
        return calculate_derivatives(plan=plan, sequence=sequence)
    elif not isinstance(existing_plan, type(plan)):
        # NOTE: A plan with the same ID existing that is not part of a workflow file, so, we cannot form a derivation
        # chain. Re-call with the next sequence ID.
        # should be improbable. Just change the ID. This also makes it impossible to ever form a derivation chain.
        sequence_value = get_next_sequence_value()
        plan.assign_new_id(sequence=sequence_value)
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
        self.command: str = ""

    def parse_command(self) -> None:
        """Parse the command and assign some properties (e.g. position) for parameters and the command."""
        self.processed_parameters = []

        try:
            # NOTE: Parse the command to an AST
            parsed_commands = bashlex.parse(self.step.command)
        except ParsingError as e:
            raise errors.ParseError(f"Cannot parse command {self.step.command}: {e}")

        if len(parsed_commands) > 1:
            raise errors.ParseError(f"Command must be a single line: {self.step.command}")

        parsed_command = parsed_commands[0]

        # NOTE: Process each token in the AST
        self.visit(parsed_command)

        # NOTE: Set step's command
        self.step.command = self.command

        unprocessed_nodes = [self.get_node_as_string(n) for n in parsed_command.parts if not n.processed]
        if unprocessed_nodes:
            nodes_str = "\n\t".join(unprocessed_nodes)
            communication.warn(
                f"The following command tokens weren't processed in step '{self.step.name}':\n\t{nodes_str}"
            )

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
            parameters_str = "\n\t".join(unprocessed_parameters)
            communication.warn(
                f"The following inputs/outputs/parameters didn't appear in the command of step '{self.step.name}': "
                f"'{self.step.original_command}':"
                f"\n(set 'implicit' attribute for them to 'true' if this is intentional)\n\t{parameters_str}"
            )

    def visitnode(self, node):
        """Start of processing a single command token."""
        if node.kind in ["list", "pipeline"]:
            raise errors.ParseError(
                f"Cannot run multiple commands/piping in step '{self.step.name}': {self.step.command}"
            )
        if node.kind not in ("command", "word", "redirect", "tilde", "parameter"):
            message = self.get_node_as_string(node)
            raise errors.ParseError(
                f"Unsupported command token: '{message}' in step '{self.step.name}': {self.step.command}"
            )

        # NOTE: Set a marker for each node if not already set
        if getattr(node, "processed", None) is None:
            node.processed = False

    def get_node_as_string(self, node) -> str:
        """Return user-friendly error message for ``bashlex`` nodes."""
        return f"Node {node.kind}: {self.step.command[node.pos[0]:node.pos[1]]}"

    def visitcommand(self, node, parts):
        """Process a full command.

        NOTE: There is exactly one command node since we don't allow piping or multiple commands. Therefore, this method
        is called only once.
        """
        if not parts:
            message = self.get_node_as_string(node)
            raise errors.ParseError(f"No command to parse in step '{self.step.name}': {message}")

        node.processed = True

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
                    raise errors.ParseError(f"Cannot find a parameter with name '{name}' in step '{self.step.name}'")
                argument.position = get_position(node)
                self.processed_parameters.append(argument)

            return True

        command: str = ""
        command_done: bool = False
        prefix: str = ""

        for index, node in enumerate(parts):
            # NOTE: We assume all redirections come at the end of the command. So, stop processing once reaching a
            # ``redirect`` node. Redirects are processed separately.
            if node.kind == "redirect":
                break

            if node.kind == "word":
                node.processed = True
                word = node.word
                assert isinstance(word, str), f"Token value isn't string: {node} in step '{self.step.name}'"

                if process_variable(token=word, node=node):
                    continue

                word = self.unescape_dollar_sign(word)

                # NOTE: If no command has not been found yet, don't search in the outputs
                argument = self.find_argument(
                    prefix=prefix,
                    value=word,
                    search_outputs=bool(command),
                    exclude_prefix=False,
                )

                # NOTE: Search with no ``prefix`` if we cannot find a match and command is not done yet.
                if not argument and not command_done:
                    argument = self.find_argument(
                        prefix=None,
                        value=word,
                        search_outputs=bool(command),
                        exclude_prefix=True,
                    )

                if argument:
                    command_done = True
                    prefix = ""
                    argument.position = get_position(node)
                    self.processed_parameters.append(argument)
                elif word == "--":
                    # NOTE: Command ends with we see ``--``
                    command_done = True
                    if prefix:
                        command = f"{command} {prefix}"
                        prefix = ""
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
                    # NOTE: The prefix was (probably) a parameter that wasn't defined
                    raise errors.ParseError(
                        f"Cannot find an argument for '{prefix}' in step '{self.step.name}': {self.step.command}"
                    )
                else:
                    prefix = word
            elif node.kind == "tilde":
                # NOTE: ``tilde`` nodes appear when there is a path with ``~`` (e.g. ~/data.csv)
                node.processed = True

        if prefix:
            if not command_done:
                command = f"{command} {prefix}"
            else:
                raise errors.ParseError(
                    f"Cannot find an argument for '{prefix}' in step '{self.step.name}': {self.step.command}"
                )

        self.command = command

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
                raise errors.ParseError(
                    f"Unsupported output redirection '{input}{type}' in step '{self.step.name}': {self.step.command}"
                )

            stream = "stdout" if input is None or input == 1 else "stderr"  # input == 2
            parameter: Optional[BasePath]

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
                    parameter = cast(BasePath, self.find_argument_by_name(name=name, collection=self.step.outputs))
                    if not parameter:
                        raise errors.ParseError(
                            f"Cannot find variable '{name}' for {stream} in step '{self.step.name}'"
                        )
            else:
                path = output.word
                parameter = self.find_output(path=path, prefix=None, exclude_prefix=False)
                if not parameter:
                    raise errors.ParseError(
                        f"Cannot find '{stream}' == '{path}' in outputs in step '{self.step.name}': {self.step.command}"
                    )

            parameter.mapped_to = stream
        elif type == "<":  # stdin
            if is_parameter(node):
                name = node.output.word.replace("$", "", 1)
                name = self.unescape_dollar_sign(name)
                if name == "inputs":
                    if len(self.step.inputs) != 0:
                        raise errors.ParseError(
                            f"There must be exactly one input when using '$inputs' as stdin in step '{self.step.name}'"
                        )
                    parameter = self.step.inputs[0]
                else:
                    parameter = cast(BasePath, self.find_argument_by_name(name=name, collection=self.step.inputs))
                    if not parameter:
                        raise errors.ParseError(f"Cannot find variable '{name}' for stdin")
            else:
                path = output.word
                parameter = self.find_input(path=path, prefix=None, exclude_prefix=False)
                if not parameter:
                    raise errors.ParseError(
                        f"Cannot find 'stdin' == '{path}' in inputs in step '{self.step.name}': {self.step.command}"
                    )

            parameter.mapped_to = "stdin"
        elif type in (">&", "<&"):
            # NOTE: Renku metadata doesn't support a stream that is mapped to another
            stream = f"{to_string(input, strip=True)}{type}{output}"
            raise errors.ParseError(
                f"Stream-to-stream redirection '{stream}' isn't supported in step '{self.step.name}': "
                f"{self.step.command}"
            )
        elif type == ">>":
            stream = f"{to_string(input, strip=True)}{type}{output}"
            raise errors.ParseError(
                f"Appending redirection '{stream}' is not supported in step '{self.step.name}': {self.step.command}"
            )
        else:
            raise errors.ParseError(
                f"Unsupported redirection operator '{type}' in step '{self.step.name}': {self.step.command}"
            )

        node.processed = True
        node.output.processed = True

        parameter.position = node.pos[0]
        self.processed_parameters.append(parameter)

    @staticmethod
    def unescape_dollar_sign(word: str) -> str:
        """Unescape $ in the command."""
        return word.replace("$$", "$")

    def find_argument_by_name(self, name: str, collection=None) -> Optional[BaseParameter]:
        """Find an inputs/outputs/parameters with the given name."""
        collection = collection or itertools.chain(self.step.inputs, self.step.outputs, self.step.parameters)
        argument = next((p for p in collection if p.name == name), None)
        if argument:
            self.processed_parameters.append(argument)

        return argument

    def find_argument(
        self, prefix: Optional[str], value: str, search_outputs: bool, exclude_prefix: bool
    ) -> Optional[BaseParameter]:
        """Check the argument against inputs/outputs/parameters to find its type.

        Args:
            prefix(Optional[str]): Argument prefix.
            value(str): Argument value.
            search_outputs(bool): Whether to search outputs or not. When no part of a command hasn't found yet, we
                shouldn't search outputs.
            exclude_prefix(bool): Whether prefix must be excluded in search or not. When the command hasn't been fully
                found, the prefix might be part of the command and not the argument.

        Returns:
            Optional[BaseParameter]: An Input, Output, or Parameter instance if a match is found; None otherwise.
        """
        parameter = self.find_parameter(prefix=prefix, value=value, exclude_prefix=exclude_prefix)
        if parameter:
            return parameter

        input = self.find_input(prefix=prefix, path=value, exclude_prefix=exclude_prefix)
        if input:
            return input

        if search_outputs:
            output = self.find_output(prefix=prefix, path=value, exclude_prefix=exclude_prefix)
            if output:
                return output

        return None

    def find_parameter(self, prefix: Optional[str], value: str, exclude_prefix: bool) -> Optional[Parameter]:
        """Find a parameter based on its value and prefix; adjust parameter's prefix if needed."""
        return self.find_and_process_argument(
            cls=Parameter,
            collection=self.step.parameters,
            prefix=prefix,
            value=value,
            exclude_prefix=exclude_prefix,
            are_values_equal=lambda a, b: a == b,
        )

    def find_input(self, prefix: Optional[str], path: str, exclude_prefix: bool) -> Optional[BasePath]:
        """Search for a given path in inputs."""
        return self.find_and_process_argument(
            cls=Input,
            collection=self.step.inputs,
            prefix=prefix,
            value=path,
            exclude_prefix=exclude_prefix,
            are_values_equal=are_paths_equal,
        )

    def find_output(self, prefix: Optional[str], path: str, exclude_prefix: bool) -> Optional[BasePath]:
        """Search for a given path in outputs."""
        return self.find_and_process_argument(
            cls=Output,
            collection=self.step.outputs,
            prefix=prefix,
            value=path,
            exclude_prefix=exclude_prefix,
            are_values_equal=are_paths_equal,
        )

    @overload
    def find_and_process_argument(
        self,
        *,
        cls: Type[BaseParameterType],
        collection: Sequence[Parameter],
        prefix: Optional[str],
        value: str,
        exclude_prefix: bool,
        are_values_equal: Callable[[str, str], bool],
    ) -> Optional[Parameter]:
        ...

    @overload
    def find_and_process_argument(
        self,
        *,
        cls: Type[BaseParameterType],
        collection: Sequence[BasePath],
        prefix: Optional[str],
        value: str,
        exclude_prefix: bool,
        are_values_equal: Callable[[str, str], bool],
    ) -> Optional[BasePath]:
        ...

    def find_and_process_argument(
        self,
        *,
        cls: Type[BaseParameterType],
        collection: Sequence[Union[BasePath, Parameter]],
        prefix: Optional[str],
        value: str,
        exclude_prefix: bool,
        are_values_equal: Callable[[str, str], bool],
    ) -> Optional[Union[BasePath, Parameter]]:
        """Find the given input/output/parameter in the collection."""
        prefix = to_string(prefix, strip=True)
        value = to_string(value, strip=True)

        attribute = "value" if cls == Parameter else "path"

        # NOTE: Search for cases that prefix can be part of the command and not the argument
        if exclude_prefix:
            for parameter in collection:
                parameter_value = to_string(getattr(parameter, attribute), strip=True)

                if not parameter.prefix and are_values_equal(value, parameter_value):
                    return parameter
            return None

        has_equal = False
        new_prefix, new_value = "", ""
        if "=" in value and not prefix:
            has_equal = True
            new_prefix, new_value = value.split("=", maxsplit=1)
            new_prefix = new_prefix.strip()

        for parameter in collection:
            # NOTE: Process each parameter only once
            if parameter in self.processed_parameters:
                continue

            parameter_prefix = to_string(parameter.prefix, strip=True)
            parameter_value = to_string(getattr(parameter, attribute), strip=True)

            separator = "=" if prefix.endswith("=") else " "

            if prefix.strip(separator) == parameter_prefix.strip(separator) and are_values_equal(
                value, parameter_value
            ):
                if parameter.prefix:
                    # NOTE: Add a separator between prefix and value
                    parameter.prefix = parameter_prefix.strip(separator) + separator
                return parameter
            elif (
                has_equal and new_prefix == parameter_prefix.strip("=") and are_values_equal(new_value, parameter_value)
            ):
                if parameter.prefix:
                    # NOTE: Add a separator between prefix and value
                    parameter.prefix = parameter_prefix.strip("=") + "="
                return parameter

        # NOTE: Do this in a second loop since the previous comparisons have higher priority
        for parameter in collection:
            # NOTE: Process each parameter only once
            # NOTE: This only concerns parameters that have value but no prefix
            if parameter in self.processed_parameters or parameter.prefix:
                continue

            # NOTE: Use ``split`` to ignore multiple whitespaces in comparison
            parameter_value = to_string(getattr(parameter, attribute), strip=True).split()  # type: ignore
            if parameter_value == f"{prefix}{value}".split() or parameter_value == f"{prefix} {value}".split():
                return parameter

        return None
