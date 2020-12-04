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
import re
from functools import reduce


def is_ascii(data):
    """Check if provided string contains only ascii characters."""
    return len(data) == len(data.encode())


def normalize_to_ascii(input_string, sep="-"):
    """Adjust chars to make the input compatible as scm source."""
    replace_all = [sep, "_", "."]
    for replacement in replace_all:
        input_string = input_string.replace(replacement, " ")

    return (
        sep.join(
            [
                component
                for component in re.sub(r"[^a-zA-Z0-9_.-]+", " ", input_string).split(" ")
                if component and is_ascii(component)
            ]
        )
        .lower()
        .strip(sep)
    )


def git_unicode_unescape(s, encoding="utf-8"):
    """Undoes git/gitpython unicode encoding."""
    if s.startswith('"'):
        return s.strip('"').encode("latin1").decode("unicode-escape").encode("latin1").decode(encoding)
    return s


def shorten_message(message, max_length=100, cut=True):
    """Shortens or wraps a commit message to be at most `max_len` characters per line."""

    if len(message) < max_length:
        return message

    if cut:
        return message[: max_length - 3] + "..."

    lines = message.split(" ")
    lines = [
        line
        if len(line) < max_length
        else "\n\t".join(line[o : o + max_length] for o in range(0, len(line), max_length))
        for line in lines
    ]

    return reduce(
        lambda c, x: (f"{c[0]} {x}", c[1] + len(x) + 1) if c[1] + len(x) <= max_length else (f"{c[0]}\n\t" + x, len(x)),
        lines,
        ("", 0),
    )
