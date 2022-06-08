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
    from renku.command.view_model.plan import PlanViewModel

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


def print_markdown(text: str):
    """Print markdown text to console."""
    from rich.console import Console
    from rich.markdown import Markdown

    Console().print(Markdown(text))


def print_plan(plan: "PlanViewModel", err: bool = False):
    """Print a plan to stderr.

    Args:
        plan(PlanViewModel): The plan to print.
        err(bool,optional): Print to ``stderr`` (Default value = False).
    """

    def print_key_value(key, value, print_empty: bool = True):
        if print_empty or value:
            click.echo(style_key(key) + style_value(value), err=err)

    def print_key(key):
        click.echo(style_key(key), err=err)

    def print_value(value):
        click.echo(style_value(value), err=err)

    def print_description(description):
        if description:
            click.echo(f"\t\t{description}", err=err)

    print_key_value("Id: ", plan.id)
    print_key_value("Name: ", plan.name)

    if plan.description:
        print_markdown(plan.description)

    print_key_value("Command: ", plan.full_command)
    print_key_value("Success Codes: ", plan.success_codes)

    if plan.annotations:
        print_key_value("Annotations:\n", plan.annotations)

    if plan.inputs:
        print_key("Inputs:")
        for run_input in plan.inputs:
            print_value(f"\t- {run_input.name}:")
            print_description(run_input.description)
            print_key_value("\t\tDefault Value: ", run_input.default_value)
            print_key_value("\t\tPosition: ", run_input.position, print_empty=False)
            print_key_value("\t\tPrefix: ", run_input.prefix, print_empty=False)

    if plan.outputs:
        print_key("Outputs:")
        for run_output in plan.outputs:
            print_value(f"\t- {run_output.name}:")
            print_description(run_output.description)
            print_key_value("\t\tDefault Value: ", run_output.default_value)
            print_key_value("\t\tPosition: ", run_output.position, print_empty=False)
            print_key_value("\t\tPrefix: ", run_output.prefix, print_empty=False)

    if plan.parameters:
        print_key("Parameters:")
        for run_parameter in plan.parameters:
            print_value(f"\t- {run_parameter.name}:")
            print_description(run_parameter.description)
            print_key_value("\t\tDefault Value: ", run_parameter.default_value)
            print_key_value("\t\tPosition: ", run_parameter.position, print_empty=False)
            print_key_value("\t\tPrefix: ", run_parameter.prefix, print_empty=False)
