# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

import contextlib
import os
import posixpath
import re
import shutil
import sys
import time
import urllib
from functools import reduce
from pathlib import Path
from subprocess import PIPE, SubprocessError, run
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union, cast
from uuid import uuid4

from renku.core import errors

if TYPE_CHECKING:
    from renku.domain_model.entity import Collection, Entity
    from renku.domain_model.git import GitURL
    from renku.domain_model.provenance.agent import Person, SoftwareAgent
    from renku.infrastructure.repository import Commit, Remote, Repository


COMMIT_DIFF_STRATEGY = "DIFF"
STARTED_AT = int(time.time() * 1e3)

BRANCH_NAME_LIMIT = 250
CLI_GITLAB_ENDPOINT = "repos"
PROTECTED_BRANCH_PREFIX = "renku/autobranch"
RENKU_BACKUP_PREFIX = "renku-backup"


# TODO: Make sure caching is thread-safe
_entity_cache: Dict[Tuple[Optional[str], str], Union["Entity", "Collection"]] = {}


def run_command(command, *paths, separator=None, **kwargs):
    """Execute command by splitting `paths` to make sure that argument list will be within os limits.

    Args:
        command: A list or tuple containing command and its arguments.
        *paths: Paths to run on.
        separator: Separator for `paths` if they need to be passed as string. (Default value = None)
    Raises:
        errors.GitError: If a Git subcommand failed.
    Returns:
        Result of last invocation.

    """
    from renku.infrastructure.repository import split_paths

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
    """Return if is a git repository and has a valid HEAD.

    Args:
        repository(Optional[Repository]): The repository to check.

    Returns:
        bool: Whether or not this is a valid Git repository.

    """
    return repository is not None and repository.head.is_valid()


def get_hook_path(path: Path, name: str) -> Path:
    """Return path to the given named hook in the given repository.

    Args:
        path(Path): The current Git repository's path.
        name(str): The name of the hook.

    Returns:
        Path: Path to the hook.

    """
    return path / ".git" / "hooks" / name


def get_oauth_url(url, gitlab_token):
    """Format URL with a username and password.

    Args:
        url: The URL to format.
        gitlab_token: The Gitlab OAuth2 Token.

    Returns:
        The URL with credentials added.
    """
    parsed_url = urllib.parse.urlparse(url)

    if not parsed_url.netloc:
        raise ValueError(f"Invalid http git url: {url}")

    netloc = f"oauth2:{gitlab_token}@{parsed_url.netloc}"
    return parsed_url._replace(netloc=netloc).geturl()


def get_cache_directory_for_repository(url) -> Path:
    """Return a path to project's cache directory.

    Args:
        url: The repository URL.

    Returns:
        Path: The path of the cache.

    """
    from renku.core.constant import CACHE
    from renku.domain_model.project_context import project_context

    return project_context.metadata_path / CACHE / get_full_repository_path(url)


def parse_git_url(url: Optional[str]) -> "GitURL":
    """Return parsed git url.

    Args:
        url(Optional[str]): The URL to parse.
    Raises:
        errors.InvalidGitURL: If ``url`` is empty.
    Returns:
        GitURL: The parsed GitURL.

    """
    from renku.domain_model.git import GitURL

    if not url:
        raise errors.InvalidGitURL("No URL provided.")

    return GitURL.parse(url)


def have_same_remote(url1, url2) -> bool:
    """Checks if two git urls point to the same remote repo ignoring protocol and credentials.

    Args:
        url1: The first URL.
        url2:The second URL.

    Returns:
        bool: True if both URLs point to the same repository.
    """
    try:
        u1 = parse_git_url(url1)
        u2 = parse_git_url(url2)

        return u1.hostname == u2.hostname and u1.path == u2.path
    except (errors.GitError, AttributeError):
        return False


def get_renku_repo_url(remote_url, deployment_hostname=None, access_token=None):
    """Return a repo url that can be authenticated by renku.

    Args:
        remote_url: The repository URL.
        deployment_hostname: The host name used by this deployment (Default value = None).
        access_token: The OAuth2 access token (Default value = None).

    Returns:
        The Renku repository URL with credentials.
    """
    parsed_remote = parse_git_url(remote_url)
    path = parsed_remote.path.strip("/")
    if path.startswith("gitlab/"):
        path = path.replace("gitlab/", "")
    path = posixpath.join(CLI_GITLAB_ENDPOINT, path)

    credentials = f"renku:{access_token}@" if access_token else ""
    hostname = deployment_hostname or parsed_remote.hostname

    if hostname.startswith("gitlab."):
        hostname = hostname.replace("gitlab.", "", 1)

    return urllib.parse.urljoin(f"https://{credentials}{hostname}", path)


def create_backup_remote(repository: "Repository", remote_name: str, url: str) -> Tuple[str, bool, Optional["Remote"]]:
    """Create a backup for ``remote_name`` and sets its url to ``url``.

    Args:
        repository(Repository): The current repository.
        remote_name(str): The name of the backup remote.
        url(str): The remote URL.

    Returns:
        Tuple[str, bool, Optional[Remote]]: Tuple of backup remote name, whether it existed already and the created
            remote if successful.
    """
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
    """Extract hostname/path of a git repository from its URL.

    Args:
        url(Optional[str]): The URL.

    Returns:
        The hostname plus path extracted from the URL.
    """
    if str is None:
        return ""

    parsed_url = parse_git_url(url)
    return posixpath.join(parsed_url.hostname, parsed_url.path)  # type:ignore


def get_repository_name(url: str) -> str:
    """Extract name of a git repository from its URL.

    Args:
        url(str): The URL to get the repository name from.

    Returns:
        str: The repository name.
    """
    return Path(get_renku_repo_url(url)).stem


def get_committer_agent(commit: "Commit") -> "SoftwareAgent":
    """Return committer SoftwareAgent.

    Args:
        commit(Commit): The commit to check.

    Returns:
        SoftwareAgent: The agent responsible for the commit.
    """
    from renku.domain_model.provenance.agent import SoftwareAgent

    return SoftwareAgent(id=commit.committer.email, name=commit.committer.name)


def get_git_user(repository: Optional["Repository"]) -> Optional["Person"]:
    """Return git user.

    Args:
        repository(Optional[Repository]): The Git repository.

    Returns:
        Optional[Person]: The person associated with the repository.

    """
    from renku.domain_model.provenance.agent import Person

    if repository is None:
        return None

    user = repository.get_user()
    return Person(name=user.name, email=user.email)


def get_remote(
    repository: Optional["Repository"], *, name: Optional[str] = None, url: Optional[str] = None
) -> Optional["Remote"]:
    """Return repository's remote using its name or url or return default remote if any.

    Args:
        repository(Optional[Repository]): The Git repository.
        name(str, optional): The name of the remote (Default value = None).
        url(str, optional): The remote URL (Default value = None).

    Returns:
        Optional[Remote]: The remote, if found.

    """
    if not repository or len(repository.remotes) == 0:
        return None
    elif name:
        return next((r for r in repository.remotes if r.name == name), None)
    elif url:
        return next((r for r in repository.remotes if r.url == url), None)
    elif len(repository.remotes) == 1:
        return repository.remotes[0]
    elif repository.active_branch and repository.active_branch.remote_branch:
        return repository.active_branch.remote_branch.remote

    return None


def check_global_git_user_is_configured():
    """Check that git user information is configured."""
    from renku.infrastructure.repository import Repository

    try:
        Repository.get_global_user()
    except errors.GitConfigurationError:
        return False
    else:
        return True


def is_path_safe(path: Union[Path, str]) -> bool:
    """Check if the path should be used in output.

    Args:
        path(Union[Path, str]): The path to check.

    Returns:
        bool: True if the path is safe else False.
    """
    path = str(path)

    # Should not be in ignore paths.
    if path in {".gitignore", ".gitattributes"}:
        return False

    # Ignore everything in .renku ...
    if path.startswith(".renku"):
        return False

    return True


def get_entity_from_revision(
    repository: "Repository", path: Union[Path, str], revision: Optional[str] = None, bypass_cache: bool = False
) -> "Entity":
    """Return an Entity instance from given path and revision.

    Args:
        repository(Repository): The current repository.
        path(Union[Path, str]): The path of the entity.
        revision(str, optional): The revision to check at (Default value = None).
        bypass_cache(bool): Whether to ignore cached entries and get information from disk (Default value = False).

    Returns:
        Entity: The Entity for the given path and revision.

    """
    from renku.domain_model.entity import Collection, Entity

    def get_directory_members(absolute_path: Path) -> List[Entity]:
        """Return first-level files/directories in a directory."""
        members: List[Entity] = []

        for member in absolute_path.iterdir():
            if member.name == ".gitkeep":
                continue

            member_path = member.relative_to(repository.path)

            assert all(member_path != m.path for m in members)

            entity = get_entity_from_revision(repository, member_path, revision, bypass_cache=bypass_cache)
            # NOTE: If a path is not found at a revision we assume that it didn't exist at that revision
            if entity:
                members.append(entity)

        return members

    global _entity_cache
    key = (revision, str(path))
    cached_entry = _entity_cache.get(key)
    if cached_entry and not bypass_cache:
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
        entity: Union[Entity, Collection] = Collection(id=id, checksum=checksum, path=path, members=members)
    else:
        entity = Entity(id=id, checksum=checksum, path=path)

    _entity_cache[key] = entity

    return entity


def get_git_path(path: Union[Path, str] = ".") -> Path:
    """Return the repository path."""
    # TODO: Implement this using ``git rev-parse --git-dir``
    try:
        path = get_git_repository(path=path).path
    except ValueError:
        path = Path(path)

    return path.resolve()


def get_git_repository(path: Union[Path, str] = ".") -> "Repository":
    """Get Git repository from the current path or any of its parents.

    Args:
        path: Path to start from (Default value = ".").
    Raises:
        ValueError: If not inside a git repository.
    Returns:
        Git repository
    """
    from renku.infrastructure.repository import Repository

    try:
        return Repository(path, search_parent_directories=True)
    except errors.GitError:
        raise ValueError(f"Cannot find a git repository at '{path}'")


def commit_changes(*paths: Union[Path, str], repository: "Repository", message=None) -> List[str]:
    """Commit paths to the repository.

    Args:
        *paths(Union[Path, str]): The paths to commit.
        repository(Repository): The repository to commit to.
        message: The commit message (Default value = None).
    Raises:
        errors.GitError: If paths couldn't be committed.
    Returns:
        List of paths that were committed.
    """
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
                paths_with_lens = cast(
                    List[Tuple[str, int]],
                    reduce(
                        lambda c, x: c + [(x, c[-1][1] + len(x))],
                        saved_paths,
                        cast(List[Tuple[Optional[str], int]], [(None, len(message))]),
                    )[1:],
                )
                # limit first line to max_len characters
                message += " ".join(p if l < max_len else "\n\t" + p for p, l in paths_with_lens)

            repository.commit(message)
    except errors.GitCommandError as e:
        raise errors.GitError("Cannot commit changes") from e
    else:
        return saved_paths


def push_changes(repository: "Repository", remote: Optional[str] = None, reset: bool = True) -> str:
    """Push to a remote branch. If the remote branch is protected a new remote branch will be created and pushed to.

    Args:
        repository(Repository): The current repository.
        remote(str, optional): The remote to push to (Default value = None).
        reset(bool, optional): Whether to reset active branch to its upstream branch, used if changes get
            pushed to a temporary branch (Default value = True).
    Raises:
        errors.GitError: If there's no remote or the push fails.
    Returns:
        str: Name of the branch that was pushed to.
    """
    from renku.core.util import communication

    if repository.active_branch is None:
        raise errors.GitError("Cannot push changes when repository is in detached HEAD state.")

    if repository.active_branch.remote_branch:
        ref = repository.active_branch.remote_branch.name
        pushed_branch = ref.split("/")[-1]
    else:
        pushed_branch = repository.active_branch.name

    if remote is not None:
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
            last_short_sha = repository.head.commit.hexsha[0:8]
            old_active_branch = str(repository.active_branch)
            fixed_chars_len = len(PROTECTED_BRANCH_PREFIX) + len(last_short_sha) + 2
            if len(old_active_branch) + fixed_chars_len > BRANCH_NAME_LIMIT:
                old_branch_reference = old_active_branch[0 : (BRANCH_NAME_LIMIT - fixed_chars_len)]
            else:
                old_branch_reference = old_active_branch
            pushed_branch = f"{PROTECTED_BRANCH_PREFIX}/{old_branch_reference}/{last_short_sha}"
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
    path: Optional[Union[Path, str]],
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
    """Clone a Renku Repository.

    Args:
        url(str): The Git URL to clone.
        path(Union[Path, str]): The path to clone into.
        gitlab_token: The gitlab OAuth2 token (Default value = None).
        deployment_hostname: The hostname of the current renku deployment (Default value = None).
        depth(Optional[int], optional): The clone depth, number of commits from HEAD (Default value = None).
        install_githooks: Whether to install git hooks (Default value = False).
        install_lfs: Whether to install Git LFS (Default value = True).
        skip_smudge: Whether to pull files from Git LFS (Default value = True).
        recursive: Whether to clone recursively (Default value = True).
        progress: The GitProgress object (Default value = None).
        config(Optional[dict], optional): Set configuration for the project (Default value = None).
        raise_git_except: Whether to raise git exceptions (Default value = False).
        checkout_revision: The revision to checkout after clone (Default value = None).
        use_renku_credentials(bool, optional): Whether to use Renku provided credentials (Default value = False).
        reuse_existing_repository(bool, optional): Whether to clone over an existing repository (Default value = False).

    Returns:
        The cloned repository.
    """
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
    path: Optional[Union[Path, str]] = None,
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
    clone_options: Optional[List[str]] = None,
) -> "Repository":
    """Clone a Git repository and install Git hooks and LFS.

    Args:
        url: The Git URL to clone.
        path(Union[Path, str], optional): The path to clone into (Default value = None).
        install_githooks: Whether to install git hooks (Default value = True).
        install_lfs: Whether to install Git LFS (Default value = True).
        skip_smudge: Whether to pull files from Git LFS (Default value = True).
        recursive: Whether to clone recursively (Default value = True).
        depth: The clone depth, number of commits from HEAD (Default value = None).
        progress: The GitProgress object (Default value = None).
        config(Optional[dict], optional): Set configuration for the project (Default value = None).
        raise_git_except: Whether to raise git exceptions (Default value = False).
        checkout_revision: The revision to checkout after clone (Default value = None).
        no_checkout(bool, optional): Whether to perform a checkout (Default value = False).
        clean(bool, optional): Whether to require the target folder to be clean (Default value = False).
        clone_options(List[str], optional): Additional clone options (Default value = None).

    Returns:
        The cloned repository.
    """
    from renku.core.githooks import install_githooks as install_githooks_function
    from renku.infrastructure.repository import Repository

    path = Path(path) if path else Path(get_repository_name(url))

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
        if not clean or not path:
            return
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass
        except PermissionError as e:
            raise errors.InvalidFileOperation(f"Cannot delete files in {path}: Permission denied") from e

    def check_and_reuse_existing_repository() -> Optional["Repository"]:
        if path is None or not cast(Path, path).exists():
            return None

        try:
            repository = Repository(path)
            remote = get_remote(repository, name="origin") or get_remote(repository)

            if remote and have_same_remote(remote.url, url):
                repository.reset(hard=True)
                repository.fetch(all=True, tags=True)
                # NOTE: By default we checkout remote repository's HEAD since the local HEAD might not point to
                # the default branch.
                default_checkout_revision = checkout_revision or "origin/HEAD"
                repository.checkout(default_checkout_revision)
                try:
                    repository.pull()
                except errors.GitCommandError:  # NOTE: When ref is not a branch, an error is thrown
                    pass
            else:
                # NOTE: not same remote, so don't reuse
                clean_directory()
                return None
        except errors.GitError:  # NOTE: Not a git repository, remote not found, or checkout failed
            clean_directory()
        else:
            return repository

        return None

    def clone(branch, depth):
        os.environ["GIT_LFS_SKIP_SMUDGE"] = "1" if skip_smudge else "0"

        return Repository.clone_from(
            url,
            cast(Path, path),
            branch=branch,
            recursive=recursive,
            depth=depth,
            no_checkout=no_checkout,
            progress=progress,
            clone_options=clone_options,
        )

    assert config is None or isinstance(config, dict), f"Config should be a dict not '{type(config)}'"

    existing_repository = check_and_reuse_existing_repository()
    if existing_repository is not None:
        return existing_repository

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
        install_githooks_function(force=True, path=repository.path)

    if install_lfs:
        repository.lfs.install(skip_smudge=skip_smudge)

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
            self._previous_line_length = len(self._cur_line) if self._cur_line else 0
            if (op_code & RemoteProgress.END) != 0:
                print()

        def _clear_line(self):
            print(self._previous_line_length * " ", end="\r")

    return GitProgress()


def get_file_size(repository_path: Path, path: str) -> Optional[int]:
    """Return file size for a file inside a git repository."""
    # NOTE: First try to get file size from Git LFS
    try:
        lfs_run = run(
            ("git", "lfs", "ls-files", "--name-only", "--size"),
            stdout=PIPE,
            cwd=repository_path,
            universal_newlines=True,
        )
    except SubprocessError:
        pass
    else:
        lfs_output = lfs_run.stdout.split("\n")
        # Example line format: relative/path/to/file (7.9 MB)
        pattern = re.compile(r".*\((.*)\)")
        for line in lfs_output:
            if path not in line:
                continue
            match = pattern.search(line)
            if not match:
                continue
            size_info = match.groups()[0].split()
            if len(size_info) != 2:
                continue
            try:
                size = float(size_info[0])
            except ValueError:
                continue
            unit = size_info[1].strip().lower()
            conversions = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9}
            multiplier = conversions.get(unit, None)
            if multiplier is None:
                continue
            return int(size * multiplier)

    # Return size of the file on disk
    full_path = repository_path / path
    return os.path.getsize(full_path) if full_path.exists() else None


def shorten_message(message: str, line_length: int = 100, body_length: int = 65000) -> str:
    """Wraps and shortens a commit message.

    Args:
        message(str): message to adjust.
        line_length(int, optional): maximum line length before wrapping. 0 for infinite (Default value = 100).
        body_length(int, optional): maximum body length before cut. 0 for infinite (Default value = 65000).
    Raises:
        ParameterError: If line_length or body_length < 0
    Returns:
        message wrapped and trimmed.

    """
    if line_length < 0:
        raise errors.ParameterError("the length can't be negative.", "line_length")

    if body_length < 0:
        raise errors.ParameterError("the length can't be negative.", "body_length")

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


def get_in_submodules(
    repository: "Repository", commit: "Commit", path: Union[Path, str]
) -> Tuple["Repository", "Commit", Path]:
    """Resolve filename in submodules."""
    original_path = repository.path / path
    in_vendor = str(path).startswith(".renku/vendors")

    if original_path.is_symlink() or in_vendor:
        resolved_path = original_path.resolve()

        for submodule in repository.submodules:  # type: ignore
            if not (submodule.path / ".git").exists():
                continue

            try:
                path_within_submodule = resolved_path.relative_to(submodule.path)
                commit = submodule.get_previous_commit(path=path_within_submodule, revision=commit.hexsha)
            except (ValueError, errors.GitCommitNotFoundError):
                pass
            else:
                return submodule, commit, path_within_submodule

    return repository, commit, Path(path)


def get_dirty_paths(repository: "Repository") -> Set[str]:
    """Get paths of dirty files in the repository."""
    modified_files = [item.b_path for item in repository.unstaged_changes if item.b_path]
    staged_files = [d.a_path for d in repository.staged_changes] if repository.head.is_valid() else []

    return {os.path.join(repository.path, p) for p in repository.untracked_files + modified_files + staged_files}


@contextlib.contextmanager
def with_commit(
    *,
    repository: "Repository",
    transaction_id: str,
    commit_only=None,
    commit_empty=True,
    raise_if_empty=False,
    commit_message=None,
    abbreviate_message=True,
    skip_dirty_checks=False,
):
    """Automatic commit."""
    diff_before = prepare_commit(repository=repository, commit_only=commit_only, skip_dirty_checks=skip_dirty_checks)

    yield

    finalize_commit(
        diff_before=diff_before,
        repository=repository,
        transaction_id=transaction_id,
        commit_only=commit_only,
        commit_empty=commit_empty,
        raise_if_empty=raise_if_empty,
        commit_message=commit_message,
        abbreviate_message=abbreviate_message,
    )


def prepare_commit(*, repository: "Repository", commit_only=None, skip_dirty_checks=False, skip_staging: bool = False):
    """Gather information about repo needed for committing later on."""

    def ensure_not_untracked(path):
        """Ensure that path is not part of git untracked files."""
        for file_path in repository.untracked_files:
            is_parent = (repository.path / file_path).parent == (repository.path / path)
            is_equal = str(path) == file_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(repository)

    def ensure_not_staged(path):
        """Ensure that path is not part of git staged files."""
        path = str(path)
        for file_path in repository.staged_changes:
            is_parent = str(file_path.a_path).startswith(path)
            is_equal = path == file_path.a_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(repository)

    if skip_staging:
        if not isinstance(commit_only, list) or len(commit_only) == 0:
            raise errors.OperationError("Cannot use ``skip_staging`` without specifying files to commit.")

    diff_before = set()

    if commit_only == COMMIT_DIFF_STRATEGY:
        if len(repository.staged_changes) > 0 or len(repository.unstaged_changes) > 0:
            repository.reset()

        # Exclude files created by pipes.
        diff_before = {
            file for file in repository.untracked_files if STARTED_AT - int(Path(file).stat().st_ctime * 1e3) >= 1e3
        }

    if isinstance(commit_only, list) and not skip_dirty_checks:
        for path in commit_only:
            ensure_not_untracked(path)
            ensure_not_staged(path)

    return diff_before


def finalize_commit(
    *,
    diff_before,
    repository: "Repository",
    transaction_id: str,
    commit_only=None,
    commit_empty=True,
    raise_if_empty=False,
    commit_message=None,
    abbreviate_message=True,
    skip_staging: bool = False,
):
    """Commit modified/added paths."""
    from renku.core.util.urls import remove_credentials
    from renku.infrastructure.repository import Actor
    from renku.version import __version__, version_url

    committer = Actor(name=f"renku {__version__}", email=version_url)

    change_types = {item.a_path: item.change_type for item in repository.unstaged_changes}

    if commit_only == COMMIT_DIFF_STRATEGY:
        # Get diff generated in command.
        staged_after = set(change_types.keys())

        modified_after_change_types = {item.a_path: item.change_type for item in repository.staged_changes}

        modified_after = set(modified_after_change_types.keys())

        change_types.update(modified_after_change_types)

        diff_after = set(repository.untracked_files).union(staged_after).union(modified_after)

        # Remove files not touched in command.
        commit_only = list(diff_after - diff_before)

    if isinstance(commit_only, list):
        for path_ in commit_only:
            p = repository.path / path_
            if p.exists() or change_types.get(str(path_)) == "D":
                repository.add(path_)

    if not commit_only:
        repository.add(all=True)

    try:
        diffs = [d.a_path for d in repository.staged_changes]
    except errors.GitError:
        diffs = []

    if not commit_empty and not diffs:
        if raise_if_empty:
            raise errors.NothingToCommit()
        return

    if commit_message and not isinstance(commit_message, str):
        raise errors.CommitMessageEmpty()

    elif not commit_message:
        argv = [os.path.basename(sys.argv[0])] + [remove_credentials(arg) for arg in sys.argv[1:]]

        commit_message = " ".join(argv)

    if abbreviate_message:
        commit_message = shorten_message(commit_message)

    # NOTE: Only commit specified paths when skipping staging area
    paths = commit_only if skip_staging else []
    # Ignore pre-commit hooks since we have already done everything.
    repository.commit(commit_message + transaction_id, committer=committer, no_verify=True, paths=paths)
