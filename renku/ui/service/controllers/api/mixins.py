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
import contextlib
from abc import ABCMeta, abstractmethod
from datetime import datetime
from functools import wraps
from pathlib import Path

import portalocker

from renku.core.constant import RENKU_HOME
from renku.core.errors import GitCommandError, GitConfigurationError, LockError, RenkuException, UninitializedProject
from renku.core.util.contexts import renku_project_context
from renku.infrastructure.repository import Repository
from renku.ui.service.cache.config import REDIS_NAMESPACE
from renku.ui.service.cache.models.job import Job
from renku.ui.service.cache.models.project import Project
from renku.ui.service.cache.models.user import User
from renku.ui.service.config import PROJECT_CLONE_DEPTH_DEFAULT, PROJECT_CLONE_NO_DEPTH
from renku.ui.service.controllers.utils.remote_project import RemoteProject
from renku.ui.service.errors import (
    IntermittentAuthenticationError,
    IntermittentCacheError,
    IntermittentLockError,
    ProgramRenkuError,
    UserAnonymousError,
)
from renku.ui.service.jobs.contexts import enqueue_retry
from renku.ui.service.jobs.delayed_ctrl import delayed_ctrl_job
from renku.ui.service.serializers.common import DelayedResponseRPC

PROJECT_FETCH_TIME = 30


def local_identity(method):
    """Ensure identity on local execution."""
    # noqa
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        """Implementation of method wrapper."""
        if not hasattr(self, "user") and not isinstance(getattr(self, "user", None), User):
            raise UserAnonymousError()

        return method(self, *method_args, **method_kwargs)

    return _impl


class RenkuOperationMixin(metaclass=ABCMeta):
    """Renku operation execution mixin.

    It's responsible for setting up all dependencies for an execution of a Renku operation. This includes cloning of
    project repository, syncing with remote state, delaying write operations and returning job identifier.
    """

    JOB_RESPONSE_SERIALIZER = DelayedResponseRPC()

    def __init__(
        self,
        cache,
        user_data,
        request_data,
        migrate_project=False,
        skip_lock=False,
        clone_depth=PROJECT_CLONE_DEPTH_DEFAULT,
    ):
        """Read operation mixin for controllers."""
        if user_data and "user_id" in user_data and cache is not None:
            self.user = cache.ensure_user(user_data)

        self.is_write = False
        self.migrate_project = migrate_project
        self.skip_lock = skip_lock
        self.clone_depth = clone_depth
        self.cache = cache
        self.user_data = user_data
        self.request_data = request_data

        # NOTE: Manually set `migrate_project` flag takes higher precedence
        # than `migrate_project` flag from the request. Reason for this is
        # because we reuse view controllers in the delayed renku operation jobs.
        if not self.migrate_project and isinstance(self.request_data, dict):
            self.migrate_project = self.request_data.get("migrate_project", False)

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

        from renku.ui.service.controllers.cache_migrate_project import MigrateProjectCtrl

        migrate_context = {
            "project_id": project_id,
            "skip_docker_update": True,
            "skip_template_update": True,
        }
        migration_response = MigrateProjectCtrl(
            self.cache, self.user_data, migrate_context, skip_lock=True
        ).to_response()

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

            with enqueue_retry(f"{REDIS_NAMESPACE}.delayed.ctrl.{ctrl_cls['renku_ctrl']}") as queue:
                queue.enqueue(delayed_ctrl_job, self.context, self.user_data, job.job_id, **ctrl_cls)

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
                from renku.ui.service.controllers.cache_project_clone import ProjectCloneCtrl

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
                elif self.clone_depth:
                    clone_context["depth"] = self.clone_depth

                project = ProjectCloneCtrl(self.cache, self.user_data, clone_context).project_clone()

                if not project.initialized:
                    raise UninitializedProject(project.abs_path)
            else:
                ref = self.request_data.get("ref", None)

                if ref:
                    with Repository(project.abs_path) as repository:
                        if ref != repository.active_branch.name:
                            # NOTE: Command called for different branch than the one used in cache, change branch
                            if len(repository.remotes) != 1:
                                raise RenkuException("Couldn't find remote for project in cache.")
                            origin = repository.remotes[0]
                            remote_branch = f"{origin}/{ref}"

                            with project.write_lock():
                                # NOTE: Add new ref to remote branches
                                repository.run_git_command("remote", "set-branches", "--add", origin, ref)
                                if self.migrate_project or self.clone_depth == PROJECT_CLONE_NO_DEPTH:
                                    repository.fetch(origin, ref)
                                else:
                                    repository.fetch(origin, ref, depth=self.clone_depth)

                                # NOTE: Switch to new ref
                                repository.run_git_command("checkout", "--track", "-f", "-b", ref, remote_branch)

                                # NOTE: cleanup remote branches in case a remote was deleted (fetch fails otherwise)
                                repository.run_git_command("remote", "prune", origin)

                                for branch in repository.branches:
                                    if branch.remote_branch and not branch.remote_branch.is_valid():
                                        repository.branches.remove(branch, force=True)
                                        # NOTE: Remove left-over refspec
                                        try:
                                            with repository.get_configuration(writable=True) as config:
                                                config.remove_value(f"remote.{origin}.fetch", f"origin.{branch}$")
                                        except GitConfigurationError:
                                            pass
                else:
                    self.reset_local_repo(project)

            self.context["project_id"] = project.project_id
            return self.local()

        else:
            raise RenkuException("context does not contain `project_id` or `git_url`")

    def reset_local_repo(self, project):
        """Reset the local repo to be up to date with the remote."""

        from renku.ui.service.controllers.cache_migrate_project import MigrateProjectCtrl

        # NOTE: Only do a fetch every >30s to get eventual consistency but not slow things down too much,
        # except for MigrateProject since that is likely to require to unshallow the repository
        if project.fetch_age < PROJECT_FETCH_TIME and not isinstance(self, MigrateProjectCtrl):
            return

        lock = project.write_lock()

        if self.skip_lock:
            lock = contextlib.suppress()
        try:
            with lock:
                if project.fetch_age < PROJECT_FETCH_TIME:
                    # NOTE: return immediately in case of multiple writers waiting
                    return

                with Repository(project.abs_path) as repository:
                    origin = None
                    tracking_branch = repository.active_branch.remote_branch
                    if tracking_branch:
                        origin = tracking_branch.remote
                    elif len(repository.remotes) == 1:
                        origin = repository.remotes[0]

                    if origin:
                        unshallow = self.migrate_project or self.clone_depth == PROJECT_CLONE_NO_DEPTH
                        if unshallow:
                            try:
                                # NOTE: It could happen that repository is already un-shallowed,
                                # in this case we don't want to leak git exception, but still want to fetch.
                                repository.fetch("origin", repository.active_branch, unshallow=True)
                            except GitCommandError:
                                repository.fetch("origin", repository.active_branch)

                            repository.reset(f"{origin}/{repository.active_branch}", hard=True)
                        else:
                            try:
                                # NOTE: it rarely happens that origin is not reachable. Try again if it fails.
                                repository.fetch("origin", repository.active_branch)
                                repository.reset(f"{origin}/{repository.active_branch}", hard=True)
                            except GitCommandError as e:
                                project.purge()
                                raise IntermittentCacheError(e)
                project.last_fetched_at = datetime.utcnow()
                project.save()
        except (portalocker.LockException, portalocker.AlreadyLocked, LockError) as e:
            raise IntermittentLockError() from e

    @local_identity
    def local(self):
        """Execute renku operation against service cache."""
        if self.user is None or self.cache is None:
            error = Exception("local execution is disabled")
            raise ProgramRenkuError(error)

        project = self.cache.get_project(self.user, self.context["project_id"])

        if self.skip_lock:
            lock = contextlib.suppress()
        elif self.is_write or self.migrate_project:
            lock = project.write_lock()
        else:
            lock = project.read_lock()
        try:
            with project.concurrency_lock():
                self.reset_local_repo(project)

                with lock:
                    # NOTE: Get up-to-date version of object
                    current_project = Project.load(project.project_id)
                    if not current_project.initialized:
                        raise UninitializedProject(project.abs_path)
                    if self.migrate_project:
                        self.ensure_migrated(project.project_id)

                    self.project_path = project.abs_path

                    with renku_project_context(self.project_path):
                        return self.renku_op()
        except (portalocker.LockException, portalocker.AlreadyLocked, LockError) as e:
            raise IntermittentLockError() from e

    def remote(self):
        """Execute renku operation against remote project."""
        if self.user_data and "token" not in self.user_data:
            raise IntermittentAuthenticationError()

        project = RemoteProject(self.user_data, self.request_data)

        with project.remote() as path:
            self.project_path = Path(path)

            if not (self.project_path / RENKU_HOME).exists():
                raise UninitializedProject(self.project_path)
            with renku_project_context(self.project_path):
                return self.renku_op()


class RenkuOpSyncMixin(RenkuOperationMixin, metaclass=ABCMeta):
    """Sync operation mixin.

    Extension of `RenkuOperationMixin` responsible for syncing all write operation with the remote. In case sync fails,
    it will create a new branch and push newly created branch to the remote and return the branch name to the client.
    """

    def sync(self, remote="origin"):
        """Sync with remote."""
        from renku.core.util.git import push_changes

        if self.project_path is None:
            raise RenkuException("unable to sync with remote since no operation has been executed")

        with Repository(self.project_path) as repository:
            return push_changes(repository, remote=remote)

    def execute_and_sync(self, remote="origin"):
        """Execute operation which controller implements and sync with the remote."""
        # NOTE: This will return the operation result as well as name of the branch to which it has been pushed.
        self.is_write = True

        result = self.execute_op()

        if isinstance(result, Job):
            return result, None

        if hasattr(result, "output"):
            result = result.output

        return result, self.sync(remote=remote)
