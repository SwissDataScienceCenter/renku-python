# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Client utilities."""

import contextlib
import functools
import uuid

import click
import yaml

from renku.api import LocalClient

from ._git import get_git_isolation


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


def pass_local_client(
    method=None,
    clean=None,
    up_to_date=None,
    commit=None,
    ignore_std_streams=True
):
    """Pass client from the current context to the decorated command."""
    if method is None:
        return functools.partial(
            pass_local_client,
            clean=clean,
            up_to_date=up_to_date,
            commit=commit,
            ignore_std_streams=ignore_std_streams,
        )

    def new_func(*args, **kwargs):
        ctx = click.get_current_context()
        client = ctx.ensure_object(LocalClient)
        stack = contextlib.ExitStack()

        # Handle --isolation option:
        if get_git_isolation():
            client = stack.enter_context(client.worktree())

        transaction = client.transaction(
            clean=clean,
            up_to_date=up_to_date,
            commit=commit,
            ignore_std_streams=ignore_std_streams
        )
        stack.enter_context(transaction)

        with stack:
            result = ctx.invoke(method, client, *args, **kwargs)
        return result

    return functools.update_wrapper(new_func, method)
