# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Repository cache interface."""

import os
import shutil
import uuid
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import portalocker
from marshmallow import ValidationError

from renku.command.clone import project_clone_command
from renku.core import errors
from renku.core.util.contexts import renku_project_context
from renku.core.util.os import normalize_to_ascii
from renku.domain_model.git import GitURL
from renku.infrastructure.repository import Repository
from renku.ui.service.cache import ServiceCache
from renku.ui.service.cache.models.project import Project
from renku.ui.service.cache.models.user import User
from renku.ui.service.config import PROJECT_CLONE_DEPTH_DEFAULT
from renku.ui.service.errors import IntermittentCacheError, IntermittentLockError
from renku.ui.service.interfaces.repository_cache import IRepositoryCache
from renku.ui.service.logger import service_log
from renku.ui.service.utils import normalize_git_url


class LocalRepositoryCache(IRepositoryCache):
    """Cache for project repos stored on local disk."""

    def get(
        self,
        cache: ServiceCache,
        git_url: str,
        branch: Optional[str],
        user: User,
        shallow: bool = True,
        commit_sha: Optional[str] = None,
    ) -> Project:
        """Get a project from cache (clone if necessary)."""
        if git_url is None:
            raise ValidationError("Invalid `git_url`, URL is empty", "git_url")
        # Note: walrus turns None into empty strings
        branch = branch or ""
        commit_sha = commit_sha or ""

        git_url = normalize_git_url(git_url)
        try:
            project = Project.get(
                (Project.user_id == user.user_id)
                & (Project.git_url == git_url)
                & (Project.branch == branch)
                & (Project.commit_sha == commit_sha)
            )
        except ValueError:
            # project not found in DB
            return self._clone_project(cache, git_url, branch, user, shallow, commit_sha)

        if not project.abs_path.exists():
            # cache folder doesn't exist anymore
            project.delete()
            return self._clone_project(cache, git_url, branch, user, shallow, commit_sha)

        if not shallow and project.is_shallow:
            self._unshallow_project(project, user)

        self._maybe_update_cache(project, user)

        if not project.initialized:
            raise errors.UninitializedProject(project.git_url)

        self._update_project_access_date(project)

        return project

    def evict(self, project: Project):
        """Evict a project from cache."""
        try:
            with project.write_lock():
                service_log.debug(f"purging project {project.project_id}:{project.name}")
                project.purge()
        except FileNotFoundError:
            project.delete()
        except Exception as e:
            service_log.error(f"Couldn't purge project {project.project_id}:{project.name} from cache", exc_info=e)

    def evict_expired(self):
        """Evict expired projects from cache."""
        for project in Project.all():
            if project.ttl_expired():
                self.evict(project)

    def _update_project_access_date(self, project: Project):
        """Update the access date of the project to current datetime."""
        project.accessed_at = datetime.utcnow()
        project.save()

    def _clone_project(
        self,
        cache: ServiceCache,
        git_url: str,
        branch: Optional[str],
        user: User,
        shallow: bool = True,
        commit_sha: Optional[str] = None,
    ) -> Project:
        """Clone a project to cache."""
        git_url = normalize_git_url(git_url)

        try:
            parsed_git_url = GitURL.parse(git_url)
        except UnicodeError as e:
            raise ValidationError("`git_url` contains unsupported characters", "git_url") from e
        except errors.InvalidGitURL as e:
            raise ValidationError("Invalid `git_url`", "git_url") from e

        if parsed_git_url.owner is None or parsed_git_url.name is None:
            raise ValidationError("Invalid `git_url`, missing owner or repository", "git_url")
        if branch == "":
            branch = None
        if commit_sha == "":
            commit_sha = None

        project_data = {
            "project_id": uuid.uuid4().hex,
            "owner": parsed_git_url.owner,
            "name": parsed_git_url.name,
            "slug": normalize_to_ascii(parsed_git_url.name),
            "clone_depth": PROJECT_CLONE_DEPTH_DEFAULT if shallow else None,
            "branch": branch,
            "git_url": git_url,
            "user_id": user.user_id,
            "commit_sha": commit_sha,
        }
        project = cache.make_project(user, project_data, persist=False)

        # NOTE: Create parent dir so lock file can be created.
        project.abs_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with project.write_lock(), renku_project_context(project.abs_path, check_git_path=False):
                try:
                    # NOTE: If two requests ran at the same time, by the time we acquire the lock a project might
                    # already be cloned by an earlier request.
                    found_project = Project.get(
                        (Project.user_id == user.user_id)
                        & (Project.git_url == git_url)
                        & (Project.branch == branch)
                        & (Project.commit_sha == commit_sha)
                        & (Project.project_id != project.project_id)
                    )
                except ValueError:
                    pass
                else:
                    if found_project.abs_path.exists():
                        service_log.debug(f"project already cloned, skipping clone: {git_url}")
                        self._update_project_access_date(found_project)
                        return found_project

                # clean directory in case of previous failed state
                # NOTE: we only want to delete the contents, NOT the folder itself, in case it's still referenced
                for root, dirs, files in os.walk(project.abs_path):
                    for f in files:
                        os.unlink(os.path.join(root, f))
                    for d in dirs:
                        shutil.rmtree(os.path.join(root, d))

                repo, project.initialized = (
                    project_clone_command()
                    .build()
                    .execute(
                        git_url_with_auth(project, user),
                        path=project.abs_path,
                        depth=project.clone_depth,
                        raise_git_except=True,
                        config={
                            "user.name": user.fullname,
                            "user.email": user.email,
                            "pull.rebase": False,
                        },
                        checkout_revision=commit_sha or project.branch,
                    )
                ).output
                project.save()

                service_log.debug(f"project successfully cloned: {repo}")

                if not project.initialized:
                    raise errors.UninitializedProject(project.git_url)

                return project
        except (portalocker.LockException, portalocker.AlreadyLocked, errors.LockError) as e:
            raise IntermittentLockError() from e

    def _unshallow_project(self, project: Project, user: User):
        """Turn a shallow clone into a full clone."""
        if project.commit_sha is not None:
            # NOTE: A project in a detached head state at a specific commit SHA does not make sense to be unshallowed
            return
        try:
            with project.write_lock(), Repository(project.abs_path) as repository:
                try:
                    # NOTE: It could happen that repository is already un-shallowed,
                    # in this case we don't want to leak git exception, but still want to fetch.
                    repository.fetch("origin", repository.active_branch, unshallow=True)
                except errors.GitCommandError:
                    repository.fetch("origin", repository.active_branch)

                repository.reset(f"origin/{repository.active_branch}", hard=True)
                project.clone_depth = None
                project.save()
        except (portalocker.LockException, portalocker.AlreadyLocked, errors.LockError) as e:
            raise IntermittentLockError() from e

    def _maybe_update_cache(self, project: Project, user: User):
        """Update the cache from the remote if it's out of date."""
        from renku.ui.service.controllers.api.mixins import PROJECT_FETCH_TIME

        if project.fetch_age < PROJECT_FETCH_TIME:
            return

        if project.commit_sha is not None and project.commit_sha != "":
            # NOTE: A project in a detached head state at a specific commit SHA cannot be updated
            return

        try:
            with project.write_lock(), Repository(project.abs_path) as repository:
                try:
                    # NOTE: it rarely happens that origin is not reachable. Try again if it fails.
                    repository.fetch(
                        "origin",
                        repository.active_branch,
                        depth=project.clone_depth
                        if project.clone_depth is not None and project.clone_depth > 0
                        else None,
                    )
                    repository.reset(f"origin/{repository.active_branch}", hard=True)
                except errors.GitCommandError as e:
                    project.purge()
                    raise IntermittentCacheError(e)

                project.last_fetched_at = datetime.utcnow()
                project.save()
        except (portalocker.LockException, portalocker.AlreadyLocked, errors.LockError) as e:
            raise IntermittentLockError() from e


def git_url_with_auth(project: Project, user: User):
    """Format url with auth."""
    git_url = urlparse(normalize_git_url(project.git_url))

    url = "oauth2:{}@{}".format(user.token, git_url.netloc)
    return git_url._replace(netloc=url).geturl()
