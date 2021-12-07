# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Git utility functions."""

import os
import pathlib
import shutil
import urllib
from functools import reduce
from pathlib import Path
from subprocess import SubprocessError, run
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from uuid import uuid4

from renku.core import errors
from renku.core.models.git import GitURL

if TYPE_CHECKING:
    from renku.core.metadata.repository import Commit, Remote, Repository
    from renku.core.models.entity import Entity
    from renku.core.models.provenance.agent import Person, SoftwareAgent


CLI_GITLAB_ENDPOINT = "repos"
RENKU_BACKUP_PREFIX = "renku-backup"


# TODO: Make sure caching is thread-safe
_entity_cache = {}


def run_command(command, *paths, separator=None, **kwargs):
    """Execute command by splitting `paths` to make sure that argument list will be within os limits.

    :param command: A list or tuple containing command and its arguments.
    :param paths: List of paths/long argument. This will be appended to `command` for each invocation.
    :param separator: Separator for `paths` if they need to be passed as string.
    :param kwargs: Extra arguments passed to `subprocess.run`.
    :returns: Result of last invocation.
    """
    from renku.core.metadata.repository import split_paths

    result = None

    for batch in split_paths(*paths):
        if separator:
            batch = [separator.join(batch)]

        try:
            if not isinstance(batch, list):
                batch = list(batch)
            result = run(command + batch, **kwargs)

            if result.returncode != 0:
                break
        except KeyboardInterrupt:
            raise
        except SubprocessError as e:
            raise errors.GitError(f"Cannot run command {command} : {e}")

    return result


def is_valid_git_repository(repository: Optional["Repository"]) -> bool:
    """Return if is a git repository and has a valid HEAD."""
    return repository is not None and repository.head.is_valid()


def get_hook_path(repository, name: str) -> Path:
    """Return path to the given named hook in the given repository."""
    return repository.path / ".git" / "hooks" / name


def get_oauth_url(url, gitlab_token):
    """Format URL with a username and password."""
    parsed_url = urllib.parse.urlparse(url)

    if not parsed_url.netloc:
        raise ValueError(f"Invalid http git url: {url}")

    netloc = f"oauth2:{gitlab_token}@{parsed_url.netloc}"
    return parsed_url._replace(netloc=netloc).geturl()


def get_cache_directory_for_repository(client, url) -> Path:
    """Return a path to client's cache directory."""
    return client.renku_path / client.CACHE / get_full_repository_path(url)


def parse_git_url(url: Optional[str]) -> GitURL:
    """Return parsed git url."""
    if not url:
        raise errors.InvalidGitURL("No URL provided.")

    return GitURL.parse(url)


def have_same_remote(url1, url2) -> bool:
    """Checks if two git urls point to the same remote repo ignoring protocol and credentials."""
    try:
        u1 = parse_git_url(url1)
        u2 = parse_git_url(url2)

        return u1.hostname == u2.hostname and u1.path == u2.path
    except (errors.GitError, AttributeError):
        return False


def get_renku_repo_url(remote_url, deployment_hostname=None, access_token=None):
    """Return a repo url that can be authenticated by renku."""
    parsed_remote = parse_git_url(remote_url)
    path = parsed_remote.path.strip("/")
    if path.startswith("gitlab/"):
        path = path.replace("gitlab/", "")
    path = pathlib.posixpath.join(CLI_GITLAB_ENDPOINT, path)

    credentials = f"renku:{access_token}@" if access_token else ""
    hostname = deployment_hostname or parsed_remote.hostname

    return urllib.parse.urljoin(f"https://{credentials}{hostname}", path)


def create_backup_remote(repository: "Repository", remote_name: str, url: str) -> Tuple[str, bool, Optional["Remote"]]:
    """Create a backup for ``remote_name`` and sets its url to ``url``."""
    backup_remote_name = f"{RENKU_BACKUP_PREFIX}-{remote_name}"

    backup_exists = any(backup_remote_name == r.name for r in repository.remotes)
    if backup_exists:
        return backup_remote_name, True, None

    try:
        remote = repository.remotes.add(name=backup_remote_name, url=url)
    except errors.GitCommandError:
        return backup_remote_name, False, None
    else:
        return backup_remote_name, False, remote


def get_full_repository_path(url: Optional[str]) -> str:
    """Extract hostname/path of a git repository from its URL."""
    if not str:
        return ""

    parsed_url = parse_git_url(url)
    return pathlib.posixpath.join(parsed_url.hostname, parsed_url.path)


def get_repository_name(url: str) -> str:
    """Extract name of a git repository from its URL."""
    return Path(get_renku_repo_url(url)).stem


def get_committer_agent(commit: "Commit") -> "SoftwareAgent":
    """Return committer SoftwareAgent."""
    from renku.core.models.provenance.agent import SoftwareAgent

    return SoftwareAgent(id=commit.committer.email, name=commit.committer.name)


def get_git_user(repository: Optional["Repository"]) -> Optional["Person"]:
    """Return git user."""
    from renku.core.models.provenance.agent import Person

    if repository is None:
        return

    user = repository.get_user()
    return Person(name=user.name, email=user.email)


def get_remote(repository: Optional["Repository"], *, name: str = None, url: str = None) -> Optional["Remote"]:
    """Return repository's remote using its name or url or return default remote if any."""
    if not repository or len(repository.remotes) == 0:
        return
    elif name:
        return next((r for r in repository.remotes if r.name == name), None)
    elif url:
        return next((r for r in repository.remotes if r.url == url), None)
    elif len(repository.remotes) == 1:
        return repository.remotes[0]
    elif repository.active_branch.remote_branch:
        return repository.active_branch.remote_branch.remote


def check_global_git_user_is_configured():
    """Check that git user information is configured."""
    from renku.core.metadata.repository import Repository

    try:
        Repository.get_global_user()
    except errors.GitConfigurationError:
        return False
    else:
        return True


def is_path_safe(path: Union[Path, str]) -> bool:
    """Check if the path should be used in output."""
    path = str(path)

    # Should not be in ignore paths.
    if path in {".gitignore", ".gitattributes"}:
        return False

    # Ignore everything in .renku ...
    if path.startswith(".renku"):
        return False

    return True


def get_entity_from_revision(repository: "Repository", path: Union[Path, str], revision: str = None) -> "Entity":
    """Return an Entity instance from given path and revision."""
    from renku.core.models.entity import Collection, Entity

    def get_directory_members(absolute_path: Path) -> List[Entity]:
        """Return first-level files/directories in a directory."""
        members: List[Entity] = []

        for member in absolute_path.iterdir():
            if member.name == ".gitkeep":
                continue

            member_path = member.relative_to(repository.path)

            assert all(member_path != m.path for m in members)

            entity = get_entity_from_revision(repository, member_path, revision)
            # NOTE: If a path is not found at a revision we assume that it didn't exist at that revision
            if entity:
                members.append(entity)

        return members

    global _entity_cache
    key = (revision, str(path))
    cached_entry = _entity_cache.get(key)
    if cached_entry:
        return cached_entry

    # NOTE: For untracked directory the hash is None; make sure to stage them first before calling this function.
    checksum = repository.get_object_hash(revision=revision, path=path)
    # NOTE: If object was not found at a revision it's either removed or exists in a different revision; keep the
    # entity and use revision as checksum
    checksum = checksum or revision or "HEAD"
    id = Entity.generate_id(checksum=checksum, path=path)

    absolute_path = repository.path / path
    if str(path) != "." and absolute_path.is_dir():
        members = get_directory_members(absolute_path)
        entity = Collection(id=id, checksum=checksum, path=path, members=members)
    else:
        entity = Entity(id=id, checksum=checksum, path=path)

    _entity_cache[cached_entry] = entity

    return entity


def default_path(path="."):
    """Return default repository path."""
    from renku.core.commands.git import get_git_home

    try:
        return get_git_home(path=path)
    except ValueError:
        return path


def commit_changes(*paths: Union[Path, str], repository: "Repository", message=None) -> List[str]:
    """Commit paths to the repository."""
    if len(paths) == 0:
        return []

    try:
        staged_files = {c.a_path for c in repository.staged_changes} if repository.head.is_valid() else set()
        path_to_save = set(paths) - staged_files
        repository.add(*path_to_save)
        saved_paths = [c.b_path for c in repository.staged_changes]

        if saved_paths:
            if not message:
                # Show saved files in message
                max_len = 100
                message = "Saved changes to: "
                paths_with_lens = reduce(
                    lambda c, x: c + [(x, c[-1][1] + len(x))], saved_paths, [(None, len(message))]
                )[1:]
                # limit first line to max_len characters
                message += " ".join(p if l < max_len else "\n\t" + p for p, l in paths_with_lens)

            repository.commit(message)
    except errors.GitCommandError as e:
        raise errors.GitError("Cannot commit changes") from e
    else:
        return saved_paths


def push_changes(repository: "Repository", remote: str = None, reset: bool = True) -> str:
    """Push to a remote branch. If the remote branch is protected a new remote branch will be created and pushed to.

    If ``reset`` is True, active branch will be reset to its upstream branch (if any) in case changes are pushed to a
    different branch.
    """
    from renku.core.utils import communication

    if repository.active_branch is None:
        raise errors.GitError("Cannot push changes when repository is in detached HEAD state.")

    if repository.active_branch.remote_branch:
        ref = repository.active_branch.remote_branch.name
        pushed_branch = ref.split("/")[-1]
    else:
        pushed_branch = repository.active_branch.name

    if remote:
        pushed_remote = get_remote(repository, url=remote) or get_remote(repository, name=remote)
        if not pushed_remote:
            if get_remote(repository, name="origin") is not None:
                pushed_branch = uuid4().hex
                pushed_remote_name = uuid4().hex
            else:
                pushed_remote_name = "origin"
            pushed_remote = repository.remotes.add(name=pushed_remote_name, url=remote)
    else:
        pushed_remote = get_remote(repository)
        if not pushed_remote:
            raise errors.GitRemoteNotFoundError("No remote has been set up for the current branch")

    try:
        # NOTE: Push local changes to remote branch.
        merge_conflict = False
        if len(pushed_remote.references) > 0 and repository.active_branch.remote_branch in pushed_remote.references:
            repository.fetch(pushed_remote)
            try:
                repository.pull(pushed_remote, repository.active_branch)
            except errors.GitCommandError as e:
                # NOTE: Couldn't pull, probably due to conflicts, try a merge.
                # NOTE: the error sadly doesn't tell any details.
                unmerged_blobs = repository.unmerged_blobs.values()
                conflicts = (stage != 0 for blobs in unmerged_blobs for stage, _ in blobs)
                if any(conflicts):
                    merge_conflict = True

                    if communication.confirm(
                        "There were conflicts when updating the local data with remote changes,"
                        " do you want to resolve them (if not, a new remote branch will be created)?",
                        warning=True,
                    ):
                        repository.run_git_command("mergetool", "-g")
                        repository.commit("merging conflict", no_edit=True)
                        merge_conflict = False
                    else:
                        repository.reset(hard=True)
                else:
                    raise errors.GitError("Cannot pull changes from remote") from e

        push_failed = False

        if not merge_conflict:
            # NOTE: Try pushing to remote branch which might fail if the branch is protected
            try:
                repository.push(pushed_remote, f"{repository.active_branch.name}:{pushed_branch}")
            except errors.GitCommandError:
                push_failed = True

        if merge_conflict or push_failed:
            # NOTE: Push to a new remote branch and reset the cache.
            old_active_branch = repository.active_branch
            pushed_branch = uuid4().hex
            try:
                repository.branches.add(pushed_branch)
                repository.checkout(pushed_branch)
                repository.push(pushed_remote, pushed_branch, set_upstream=True)
            except Exception:
                reset = False  # NOTE: Don't reset the repository if push to the new remote branch failed
                raise
            finally:
                repository.checkout(old_active_branch)
                if reset:
                    repository.reset(reference=repository.active_branch.remote_branch, hard=True)
    except errors.GitCommandError as e:
        raise errors.GitError("Cannot push changes") from e

    return pushed_branch


def clone_renku_repository(
    url: str,
    path: Union[Path, str],
    gitlab_token=None,
    deployment_hostname=None,
    depth: Optional[int] = None,
    install_githooks=False,
    install_lfs=True,
    skip_smudge=True,
    recursive=True,
    progress=None,
    config: Optional[dict] = None,
    raise_git_except=False,
    checkout_revision=None,
    use_renku_credentials: bool = False,
    reuse_existing_repository: bool = False,
) -> "Repository":
    """Clone a Renku Repository."""
    parsed_url = parse_git_url(url)

    clone_options = None
    create_backup = False

    if parsed_url.hostname == "localhost":
        absolute_path = Path(parsed_url.path).resolve()
        git_url = str(absolute_path)
    elif parsed_url.scheme in ["http", "https"] and gitlab_token:
        git_url = get_oauth_url(url, gitlab_token)
    elif parsed_url.scheme in ["http", "https"] and use_renku_credentials:
        clone_options = [f"--config credential.helper='!renku credentials --hostname {parsed_url.hostname}'"]
        deployment_hostname = deployment_hostname or parsed_url.hostname
        git_url = get_renku_repo_url(url, deployment_hostname=deployment_hostname, access_token=None)
        create_backup = True
    else:
        git_url = url

    repository = clone_repository(
        git_url,
        path=path,
        install_githooks=install_githooks,
        depth=depth,
        clean=reuse_existing_repository,
        install_lfs=install_lfs,
        skip_smudge=skip_smudge,
        recursive=recursive,
        progress=progress,
        config=config,
        raise_git_except=raise_git_except,
        checkout_revision=checkout_revision,
        clone_options=clone_options,
    )

    if create_backup:
        create_backup_remote(repository=repository, remote_name="origin", url=url)

    return repository


def clone_repository(
    url,
    path: Union[Path, str] = None,
    install_githooks=True,
    install_lfs=True,
    skip_smudge=True,
    recursive=True,
    depth=None,
    progress=None,
    config: Optional[dict] = None,
    raise_git_except=False,
    checkout_revision=None,
    no_checkout: bool = False,
    clean: bool = False,
    clone_options: List[str] = None,
) -> "Repository":
    """Clone a Git repository and install Git hooks and LFS."""
    from renku.core.management.githooks import install
    from renku.core.metadata.repository import Repository

    def handle_git_exception():
        """Handle git exceptions."""
        if raise_git_except:
            return

        message = f"Cannot clone repo from {url}"

        if progress:
            lines = progress.other_lines + progress.error_lines
            error = "".join([f"\n\t{line}" for line in lines if line.strip()])
            message += f" - error message:\n {error}"

        raise errors.GitError(message)

    def clean_directory():
        if not clean:
            return
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass
        except PermissionError as e:
            raise errors.InvalidFileOperation(f"Cannot delete files in {path}: Permission denied") from e

    def check_and_reuse_existing_repository() -> Optional[Repository]:
        if not path.exists():
            return

        try:
            repository = Repository(path)
            remote = get_remote(repository, name="origin") or get_remote(repository)

            if remote and have_same_remote(remote.url, url):
                repository.reset(hard=True)
                repository.fetch(all=True)
                # NOTE: By default we checkout remote repository's HEAD since the local HEAD might not point to
                # the default branch.
                default_checkout_revision = checkout_revision or "origin/HEAD"
                repository.checkout(default_checkout_revision)
                try:
                    repository.pull()
                except errors.GitCommandError:  # NOTE: When ref is not a branch, an error is thrown
                    pass
        except errors.GitError:  # NOTE: Not a git repository, remote not found, or checkout failed
            clean_directory()
        else:
            return repository

    def clone(branch, depth):
        if skip_smudge:
            os.environ["GIT_LFS_SKIP_SMUDGE"] = "1"

        return Repository.clone_from(
            url,
            path,
            branch=branch,
            recursive=recursive,
            depth=depth,
            no_checkout=no_checkout,
            progress=progress,
            clone_options=clone_options,
        )

    assert config is None or isinstance(config, dict), f"Config should be a dict not '{type(config)}'"

    path = Path(path) if path else Path(get_repository_name(url))

    repository = check_and_reuse_existing_repository()
    if repository:
        return repository

    try:
        # NOTE: Try to clone, assuming checkout_revision is a branch or a tag (if it is set)
        repository = clone(branch=checkout_revision, depth=depth)
    except errors.GitCommandError:
        if not checkout_revision:
            handle_git_exception()
            raise

        # NOTE: clone without branch set, in case checkout_revision was not a branch or a tag but a commit
        try:
            repository = clone(branch=None, depth=None)
        except errors.GitCommandError:
            handle_git_exception()
            raise

    if checkout_revision is not None and not no_checkout:
        try:
            repository.checkout(checkout_revision)
        except errors.GitCommandError:
            raise errors.GitReferenceNotFoundError(
                f"Cannot checkout reference '{checkout_revision}' in repository: {url}"
            )

    if config:
        with repository.get_configuration(writable=True) as config_writer:
            for key, value in config.items():
                try:
                    section, option = key.rsplit(".", maxsplit=1)
                except ValueError:
                    raise errors.GitError(f"Cannot write to config: Invalid config '{key}'")

                config_writer.set_value(section, option, value)

    if install_githooks:
        install(force=True, repository=repository)

    if install_lfs:
        command = ["lfs", "install", "--local", "--force"]
        if skip_smudge:
            command += ["--skip-smudge"]
        try:
            repository.run_git_command(*command)
        except errors.GitCommandError as e:
            raise errors.GitError("Cannot install Git LFS") from e

    return repository


def get_git_progress_instance():
    """Return a GitProgress object."""
    from git.remote import RemoteProgress

    class GitProgress(RemoteProgress):
        """Progress printing for GitPython."""

        def __init__(self):
            """Initialize a Git progress printer."""
            super().__init__()
            self._previous_line_length = 0

        def update(self, op_code, cur_count, max_count=None, message=""):
            """Callback for printing Git operation status."""
            self._clear_line()
            print(self._cur_line, end="\r")
            self._previous_line_length = len(self._cur_line)
            if (op_code & RemoteProgress.END) != 0:
                print()

        def _clear_line(self):
            print(self._previous_line_length * " ", end="\r")

    return GitProgress()
