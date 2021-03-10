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
from renku.service.cache.models.job import Job
from renku.service.cache.models.project import Project
from renku.service.cache.models.user import User
from renku.service.config import PROJECT_CLONE_NO_DEPTH
from renku.service.controllers.utils.remote_project import RemoteProject
from renku.service.errors import AuthenticationTokenMissing, IdentificationError, OperationNotSupported
from renku.service.jobs.contexts import enqueue_retry
from renku.service.jobs.delayed_ctrl import delayed_ctrl_job
from renku.service.serializers.common import DelayedResponseRPC


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


class RenkuOperationMixin(metaclass=ABCMeta):
    """Read operation mixin."""

    JOB_RESPONSE_SERIALIZER = DelayedResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Read operation mixin for controllers."""
        if user_data and "user_id" in user_data and cache is not None:
            self.user = cache.ensure_user(user_data)

        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

        self.migrate_project = migrate_project

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

    def ensure_migrated(self, project_id):
        """Ensure that project is migrated."""
        if not self.migrate_project:
            return

        from renku.service.controllers.cache_migrate_project import MigrateProjectCtrl

        migrate_context = {
            "project_id": project_id,
            "skip_docker_update": True,
            "skip_template_update": True,
        }
        migration_response = MigrateProjectCtrl(self.cache, self.user_data, migrate_context).to_response()

        return migration_response

    def execute_op(self):
        """Execute renku operation which controller implements."""
        ctrl_cls = {
            "renku_module": self.__class__.__module__,
            "renku_ctrl": self.__class__.__name__,
        }

        if self.context.get("is_delayed", False) and "user_id" in self.user_data:
            # NOTE: After pushing the controller to delayed execution,
            # its important to remove the delayed mark,
            # otherwise job will keep recursively enqueuing itself.
            self.context.pop("is_delayed")

            job = self.cache.make_job(self.user, job_data={"ctrl_context": {**self.context, **ctrl_cls}})

            with enqueue_retry(f"delayed.ctrl.{ctrl_cls['renku_ctrl']}") as queue:
                queue.enqueue(
                    delayed_ctrl_job, self.context, self.user_data, job.job_id, **ctrl_cls,
                )

            return job

        if "project_id" in self.context:
            return self.local()

        elif "git_url" in self.context and "user_id" not in self.user_data:
            # NOTE: Anonymous session support.
            return self.remote()

        elif "git_url" in self.context and "user_id" in self.user_data:
            try:
                project = Project.get(
                    (Project.user_id == self.user_data["user_id"]) & (Project.git_url == self.context["git_url"])
                )
            except ValueError:
                from renku.service.controllers.cache_project_clone import ProjectCloneCtrl

                clone_context = {
                    "git_url": self.request_data["git_url"],
                }

                if "ref" in self.request_data:
                    clone_context["ref"] = self.request_data["ref"]

                # NOTE: If we want to migrate project, then we need to do full clone.
                # This operation can take very long time, and as such is expected
                # only to be executed from delayed tasks.
                if self.migrate_project:
                    clone_context["depth"] = PROJECT_CLONE_NO_DEPTH

                project = ProjectCloneCtrl(self.cache, self.user_data, clone_context).project_clone()

                if not project.initialized:
                    raise UninitializedProject(project.abs_path)

                if self.migrate_project:
                    self.ensure_migrated(project.project_id)

            self.context["project_id"] = project.project_id
            return self.local()

        else:
            raise RenkuException("context does not contain `project_id` or `git_url`")

    def reset_local_repo(self, project):
        """Reset the local repo to be up to date with the remote."""
        repo = Repo(self.project_path)
        origin = None
        if repo.active_branch.tracking_branch():
            origin = repo.remotes[repo.active_branch.tracking_branch().remote_name]
        elif repo.remotes and len(repo.remotes) == 1:
            origin = repo.remotes[0]

        if origin:
            origin.fetch()
            repo.git.reset("--hard", origin)

    @local_identity
    def local(self):
        """Execute renku operation against service cache."""
        if self.user is None or self.cache is None:
            raise OperationNotSupported("local execution is disabled")

        project = self.cache.get_project(self.user, self.context["project_id"])

        if not project.initialized:
            raise UninitializedProject(project.abs_path)

        if self.migrate_project:
            self.ensure_migrated(project.project_id)

        self.project_path = project.abs_path

        self.reset_local_repo(project)

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


class RenkuOpSyncMixin(RenkuOperationMixin, metaclass=ABCMeta):
    """Sync operation mixin."""

    def sync(self, remote="origin"):
        """Sync with remote."""
        from renku.core.commands.save import repo_sync

        if self.project_path is None:
            raise RenkuException("unable to sync with remote since no operation has been executed")

        bla, remote_branch = repo_sync(Repo(self.project_path), remote=remote)

        return remote_branch

    def execute_and_sync(self, remote="origin"):
        """Execute operation which controller implements and sync with the remote."""
        # NOTE: This will return the operation result as well as name of the branch to which it has been pushed.
        result = self.execute_op()

        if isinstance(result, Job):
            return result, None

        if hasattr(result, "output"):
            result = result.output

        return result, self.sync(remote=remote)
