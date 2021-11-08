# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Helpers utils for interacting with remote source code management tools."""

from functools import reduce

from renku.core.errors import ParameterError


def shorten_message(message: str, line_length: int = 100, body_length: int = 65000):
    """Wraps and shortens a commit message.

    :param message: message to adjust.
    :param line_length: maximum line length before wrapping. 0 for infinite.
    :param body_length: maximum body length before cut. 0 for infinite.
    :return: message wrapped and trimmed.
    """

    if line_length < 0:
        raise ParameterError("the length can't be negative.", "line_length")

    if body_length < 0:
        raise ParameterError("the length can't be negative.", "body_length")

    if body_length and len(message) > body_length:
        message = message[: body_length - 3] + "..."

    if line_length == 0 or len(message) <= line_length:
        return message

    lines = message.split(" ")
    lines = [
        line
        if len(line) < line_length
        else "\n\t".join(line[o : o + line_length] for o in range(0, len(line), line_length))
        for line in lines
    ]

    # NOTE: tries to preserve message spacing.
    wrapped_message = reduce(
        lambda c, x: (f"{c[0]} {x}", c[1] + len(x) + 1)
        if c[1] + len(x) <= line_length
        else (f"{c[0]}\n\t" + x, len(x)),
        lines,
        ("", 0),
    )[0]
    return wrapped_message[1:]
