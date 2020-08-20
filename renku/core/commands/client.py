# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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

from renku.core.management import LocalClient

from ..management.config import RENKU_HOME
from ..management.migrate import check_for_migration
from ..management.repository import default_path
from .git import get_git_isolation


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


def pass_local_client(
    method=None,
    clean=None,
    requires_migration=False,
    commit=None,
    commit_only=None,
    ignore_std_streams=True,
    commit_empty=True,
    raise_if_empty=False,
    lock=None,
):
    """Pass client from the current context to the decorated command."""
    if method is None:
        return functools.partial(
            pass_local_client,
            clean=clean,
            requires_migration=requires_migration,
            commit=commit,
            commit_only=commit_only,
            ignore_std_streams=ignore_std_streams,
            commit_empty=commit_empty,
            raise_if_empty=raise_if_empty,
            lock=lock,
        )

    def new_func(*args, **kwargs):
        ctx = click.get_current_context(silent=True)
        if ctx is None:
            client = LocalClient(path=default_path(), renku_home=RENKU_HOME, external_storage_requested=True,)
            ctx = click.Context(click.Command(method))
        else:
            client = ctx.ensure_object(LocalClient)

        stack = contextlib.ExitStack()

        # Handle --isolation option:
        if get_git_isolation():
            client = stack.enter_context(client.worktree())

        if requires_migration:
            check_for_migration(client)

        transaction = client.transaction(
            clean=clean,
            commit=commit,
            commit_empty=commit_empty,
            commit_message=kwargs.get("commit_message", None),
            commit_only=commit_only,
            ignore_std_streams=ignore_std_streams,
            raise_if_empty=raise_if_empty,
        )
        stack.enter_context(transaction)

        if lock or (lock is None and commit):
            stack.enter_context(client.lock)

        result = None
        if ctx:
            with stack:
                result = ctx.invoke(method, client, *args, **kwargs)

        return result

    return functools.update_wrapper(new_func, method)
