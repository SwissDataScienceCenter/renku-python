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
"""Utility functions for ViewModels."""

import functools
import pydoc
import re
import shutil
from typing import TYPE_CHECKING

import click

from renku.ui.cli.utils import color

if TYPE_CHECKING:
    from renku.command.view_model.composite_plan import CompositePlanViewModel
    from renku.command.view_model.plan import PlanViewModel
    from renku.command.view_model.workflow_file import StepViewModel, WorkflowFileViewModel

style_header = functools.partial(click.style, bold=True, fg=color.YELLOW)
style_key = functools.partial(click.style, bold=True, fg=color.MAGENTA)
style_value = functools.partial(click.style, bold=True)

ANSI_REGEX = re.compile(r"(?:\x1B[@-Z\\-_]|[\x80-\x9A\x9C-\x9F]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])")


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI formatting codes from text."""
    return ANSI_REGEX.sub("", text)


def show_text_with_pager(text: str) -> None:
    """Print text with a pager (i.e. ``less``) if appropriate.

    Args:
        text(str): The text to print.
    """
    tty_size = shutil.get_terminal_size(fallback=(120, 40))

    if len(text.splitlines()) >= tty_size.lines:
        pydoc.pipepager(text, "less --chop-long-lines -R --tilde")
    else:
        click.echo(text)


def print_markdown(text: str, err: bool = False):
    """Print markdown text to console."""
    from rich.console import Console
    from rich.markdown import Markdown

    Console(stderr=err).print(Markdown(text))


def print_key_value(key, value, print_empty: bool = True, err: bool = False, indent: int = 0):
    """Print a key value pair."""
    if print_empty or value or (not isinstance(value, bool) and value == 0):
        click.echo(" " * indent + style_key(key) + style_value(value), err=err)


def print_key(key, err: bool = False, indent: int = 0):
    """Print a key."""
    click.echo(" " * indent + style_key(key), err=err)


def print_value(value, err: bool = False, indent: int = 0):
    """Print a value."""
    click.echo(" " * indent + style_value(value), err=err)


def print_description(description, err: bool = False, indent: int = 0):
    """Print a description."""
    if description:
        indent_str = " " * indent
        click.echo(f"{indent_str}\t\t{description}", err=err)


def print_plan(plan: "PlanViewModel", err: bool = False):
    """Print a plan to stderr.

    Args:
        plan(PlanViewModel): The plan to print.
        err(bool,optional): Print to ``stderr`` (Default value = False).
    """
    print_key_value("Id: ", plan.id, err=err)
    print_key_value("Name: ", plan.name, err=err)

    if plan.description:
        print_markdown(plan.description, err=err)

    if plan.creators:
        print_key_value("Creators: ", ", ".join(str(c) for c in plan.creators), err=err)

    if plan.keywords:
        print_key_value("Keywords: ", ", ".join(k for k in plan.keywords), err=err)

    print_key_value("Command: ", plan.full_command, err=err)
    print_key_value("Success Codes: ", plan.success_codes, err=err)

    if plan.annotations:
        print_key_value("Annotations:\n", plan.annotations, err=err)

    if plan.inputs:
        print_key("Inputs:", err=err)
        for run_input in plan.inputs:
            print_value(f"\t- {run_input.name}:", err=err)
            print_description(run_input.description, err=err)
            print_key_value("\t\tDefault Value: ", run_input.default_value, err=err)
            print_key_value("\t\tPosition: ", run_input.position, print_empty=False, err=err)
            print_key_value("\t\tPrefix: ", run_input.prefix, print_empty=False, err=err)

    if plan.outputs:
        print_key("Outputs:")
        for run_output in plan.outputs:
            print_value(f"\t- {run_output.name}:", err=err)
            print_description(run_output.description, err=err)
            print_key_value("\t\tDefault Value: ", run_output.default_value, err=err)
            print_key_value("\t\tPosition: ", run_output.position, print_empty=False, err=err)
            print_key_value("\t\tPrefix: ", run_output.prefix, print_empty=False, err=err)

    if plan.parameters:
        print_key("Parameters:", err=err)
        for run_parameter in plan.parameters:
            print_value(f"\t- {run_parameter.name}:", err=err)
            print_description(run_parameter.description, err=err)
            print_key_value("\t\tDefault Value: ", run_parameter.default_value, err=err)
            print_key_value("\t\tPosition: ", run_parameter.position, print_empty=False, err=err)
            print_key_value("\t\tPrefix: ", run_parameter.prefix, print_empty=False, err=err)


def print_composite_plan(composite_plan: "CompositePlanViewModel"):
    """Print a CompositePlan to stdout."""

    print_key_value("Id: ", composite_plan.id)
    print_key_value("Name: ", composite_plan.name)

    if composite_plan.creators:
        print_key_value("Creators: ", ", ".join(str(c) for c in composite_plan.creators))

    if composite_plan.keywords:
        print_key_value("Keywords: ", ", ".join(k for k in composite_plan.keywords))

    if composite_plan.description:
        print_markdown(composite_plan.description)

    print_key("Steps:")
    for step in composite_plan.steps:
        print_value(f"\t- {step.name}:")
        print_key_value("\t\tId: ", f"{step.id}")

    if composite_plan.mappings:
        print_key("Mappings:")
        for mapping in composite_plan.mappings:
            print_value(f"\t- {mapping.name}:")

            if mapping.description:
                print_description(mapping.description)

            print_key_value("\t\tDefault Value: ", mapping.default_value)
            print_key("\t\tMaps to: ")
            for maps_to in mapping.maps_to:
                print_value(f"\t\t\t{maps_to}")

    if composite_plan.links:
        print_key("Links: ")
        for link in composite_plan.links:
            print_key_value("\t- From: ", link.source)
            print_key("\t\t To: ")
            for sink in link.sinks:
                print_value(f"\t\t\t- {sink}")


def print_workflow_file(workflow_file: "WorkflowFileViewModel"):
    """Print a workflow file to stdout."""
    print_key_value("Name: ", workflow_file.name)
    print_key_value("Path: ", workflow_file.path)

    if workflow_file.keywords:
        print_key_value("Keywords: ", ", ".join(k for k in workflow_file.keywords))

    print_key_value("Description: ", workflow_file.description, print_empty=False)

    print_key("Steps:")
    for step in workflow_file.steps:
        print_step(step)
        click.echo()


def print_step(step: "StepViewModel"):
    """Print a step of a workflow file to stderr."""
    indent = 4
    i1 = indent
    i2 = 2 * indent
    i3 = 3 * indent
    i4 = 4 * indent

    print_value(f"{step.name}:", indent=i1)

    print_key_value("Command: ", step.full_command, indent=i2)
    print_key_value("Processed command: ", step.command, indent=i2)
    print_key_value("Success codes: ", step.success_codes, print_empty=False, indent=i2)
    if step.keywords:
        print_key_value("Keywords: ", ", ".join(k for k in step.keywords), indent=i2)
    print_key_value("Description: ", step.description, print_empty=False, indent=i2)

    if step.inputs:
        print_key("Inputs:", indent=i2)
        for input in step.inputs:
            print_value(f"{input.name}:", indent=i3)
            print_key_value("Path: ", input.path, indent=i4)
            print_key_value("Prefix: ", input.prefix, print_empty=False, indent=i4)
            print_key_value("Position: ", input.position, print_empty=False, indent=i4)
            print_key_value("Description: ", input.description, print_empty=False, indent=i4)
            print_key_value("Mapped to: ", input.mapped_to, print_empty=False, indent=i4)
            print_key_value("Implicit: ", input.implicit, print_empty=False, indent=i4)

    if step.outputs:
        print_key("Outputs:", indent=i2)
        for output in step.outputs:
            print_value(f"{output.name}:", indent=i3)
            print_key_value("Path: ", output.path, indent=i4)
            print_key_value("Prefix: ", output.prefix, print_empty=False, indent=i4)
            print_key_value("Position: ", output.position, print_empty=False, indent=i4)
            print_key_value("Description: ", output.description, print_empty=False, indent=i4)
            print_key_value("Persist: ", output.persist, print_empty=False, indent=i4)
            print_key_value("Mapped to: ", output.mapped_to, print_empty=False, indent=i4)
            print_key_value("Implicit: ", output.implicit, print_empty=False, indent=i4)

    if step.parameters:
        print_key("Parameters:", indent=i2)
        for parameter in step.parameters:
            print_value(f"{parameter.name}:", indent=i3)
            print_key_value("Value: ", parameter.value, indent=i4)
            print_key_value("Prefix: ", parameter.prefix, print_empty=False, indent=i4)
            print_key_value("Position: ", parameter.position, print_empty=False, indent=i4)
            print_key_value("Description: ", parameter.description, print_empty=False, indent=i4)
            print_key_value("Implicit: ", parameter.implicit, print_empty=False, indent=i4)
