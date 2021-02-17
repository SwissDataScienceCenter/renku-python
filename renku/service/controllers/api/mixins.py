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
"""Renku service controller mixin."""
from abc import ABCMeta, abstractmethod
from functools import wraps
from pathlib import Path

from git import Repo

from renku.core.errors import RenkuException, UninitializedProject
from renku.core.management.config import RENKU_HOME
from renku.core.management.repository import RepositoryApiMixin
from renku.core.utils.contexts import click_context
from renku.service.cache.models.user import User
from renku.service.controllers.utils.remote_project import RemoteProject
from renku.service.errors import AuthenticationTokenMissing, IdentificationError, OperationNotSupported


def local_identity(method):
    """Ensure identity on local execution."""
    # noqa
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        """Implementation of method wrapper."""
        if not hasattr(self, "user") and not isinstance(getattr(self, "user", None), User):
            raise IdentificationError("Cannot execute user operation while anonymous - user identification is missing.")

        return method(self, *method_args, **method_kwargs)

    return _impl


class ReadOperationMixin(metaclass=ABCMeta):
    """Read operation mixin."""

    def __init__(self, cache, user_data, request_data):
        """Read operation mixin for controllers."""
        if user_data and "user_id" in user_data and cache is not None:
            self.user = cache.ensure_user(user_data)

        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

        # NOTE: This is absolute project path and its set before invocation of `renku_op`,
        # so its safe to use it in controller operations. Its type will always be `pathlib.Path`.
        self.project_path = None

    @property
    @abstractmethod
    def context(self):
        """Operation context."""
        raise NotImplementedError

    @abstractmethod
    def renku_op(self):
        """Implements operation for the controller."""
        raise NotImplementedError

    def execute_op(self):
        """Execute renku operation which controller implements."""
        if "project_id" in self.context:
            return self.local()
        elif "git_url" in self.context:
            return self.remote()
        else:
            raise RenkuException("context does not contain `project_id` or `git_url`")

    @local_identity
    def local(self):
        """Execute renku operation against service cache."""
        if self.user is None or self.cache is None:
            raise OperationNotSupported("local execution is disabled")

        project = self.cache.get_project(self.user, self.context["project_id"])

        if not project.initialized:
            raise UninitializedProject(project.abs_path)

        self.project_path = project.abs_path

        with click_context(self.project_path, "renku_op"):
            return self.renku_op()

    def remote(self):
        """Execute renku operation against remote project."""
        if self.user_data and "token" not in self.user_data:
            raise AuthenticationTokenMissing()

        project = RemoteProject(self.user_data, self.request_data)

        with project.remote() as path:
            self.project_path = Path(path)

            if not (self.project_path / RENKU_HOME / RepositoryApiMixin.METADATA).exists():
                raise UninitializedProject(self.project_path)

            return self.renku_op()


class ReadWithSyncOperation(ReadOperationMixin, metaclass=ABCMeta):
    """Sync operation mixin."""

    def sync(self, remote="origin"):
        """Sync with remote."""
        from renku.core.commands.save import repo_sync

        if self.project_path is None:
            raise RenkuException("unable to sync with remote since no operation has been executed")

        _, remote_branch = repo_sync(Repo(self.project_path), remote=remote)
        return remote_branch

    def execute_and_sync(self, remote="origin"):
        """Execute operation which controller implements and sync with the remote."""
        # NOTE: This will return the operation result as well as name of the branch to which it has been pushed.
        return self.execute_op(), self.sync(remote=remote)
