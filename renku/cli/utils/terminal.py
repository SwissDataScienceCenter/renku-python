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
"""Utility functions for ViewModels."""

import contextlib
import functools
import sys
from typing import TYPE_CHECKING

import click

from renku.cli.utils import color

if TYPE_CHECKING:
    from renku.core.commands.view_model.plan import PlanViewModel


def print_markdown(text: str):
    """Print markdown text to console."""
    from rich.console import Console
    from rich.markdown import Markdown

    Console().print(Markdown(text))


def echo(message: str, err: bool = False, require_tty: bool = False, either: bool = False):
    """Print a message to std output streams.

    Args:
        message: Message to print.
        err: Whether to use stderr or not.
        require_tty: Print only if output is a terminal.
        either: Try both stdout and stderr to find a terminal; used only if ``require_tty`` is True; ignores ``err``.
    """
    if require_tty:
        if either:
            for stream in [sys.stdout, sys.stderr]:
                if stream.isatty():
                    click.echo(message=message, err=err)
                    break
        else:
            stream = sys.stderr if err else sys.stdout
            if stream.isatty():
                click.echo(message=message, err=err)
    else:
        click.echo(message=message, err=err)


def print_plan(plan: "PlanViewModel", require_tty: bool = False):
    """Print a plan to stderr.

    Args:
        require_tty: Print only if output is a terminal.
    """
    style_key = functools.partial(click.style, bold=True, fg=color.MAGENTA)
    style_value = functools.partial(click.style, bold=True)

    def print_key_value(key, value, print_empty: bool = True):
        if print_empty or value:
            echo(style_key(key) + style_value(value), require_tty=require_tty, either=True)

    def print_key(key):
        echo(style_key(key), require_tty=require_tty, either=True)

    def print_value(value):
        echo(style_value(value), require_tty=require_tty, either=True)

    def print_description(description):
        if description:
            echo(f"\t\t{description}", require_tty=require_tty, either=True)

    print_key_value("Id: ", plan.id)
    print_key_value("Name: ", plan.name)

    if plan.description:
        print_markdown(plan.description)

    print_key_value("Command: ", plan.full_command)
    print_key_value("Success Codes: ", plan.success_codes)

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
