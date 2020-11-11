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

import functools
import uuid

import yaml

from renku.core.incubation.command import Command, CommandResult

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
        cmd = Command().command(method)

        if not ignore_std_streams:
            cmd = cmd.track_std_streams()

        # Handle --isolation option:
        if get_git_isolation():
            cmd = cmd.with_git_isolation()

        if clean:
            cmd = cmd.require_clean()

        if requires_migration:
            cmd = cmd.require_migration()

        if commit:
            cmd = cmd.with_commit(commit_only=commit_only, commit_if_empty=commit_empty, raise_if_empty=raise_if_empty)

        if lock or (lock is None and commit):
            cmd = cmd.lock_project()

        cmd.build()

        result = cmd.execute(*args, **kwargs)

        if result.status == CommandResult.FAILURE:
            raise result.error

        return result.output

    return functools.update_wrapper(new_func, method)
