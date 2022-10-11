# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Custom console echo."""

import functools
import os

import click

INFO = click.style("Info: ", bold=True, fg="blue")
WARNING = click.style("Warning: ", bold=True, fg="yellow")
ERROR = click.style("Error: ", bold=True, fg="red")


def echo_via_pager(*args, **kwargs):
    """Display pager only if it does not fit in one terminal screen.

    NOTE: The feature is available only on ``less``-based pager.
    """
    restore = "LESS" not in os.environ
    try:
        os.environ.setdefault("LESS", "-iXFR")
        click.echo_via_pager(*args, **kwargs)
    finally:
        if restore:
            os.environ.pop("LESS", None)


progressbar = functools.partial(
    click.progressbar, fill_char=click.style(" ", bg="green"), show_pos=True, item_show_func=lambda x: x
)


def yellow_text(text: str) -> str:
    """Return text in yellow."""
    return click.style(text, fg="yellow")


def red_text(text: str) -> str:
    """Return text in red."""
    return click.style(text, fg="red")
