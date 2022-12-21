# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""An abstraction layer for the underlying VCS."""

import configparser
import hashlib
import itertools
import math
import os
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from itertools import zip_longest
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import git

from renku.core import errors
from renku.core.util.os import delete_dataset_file, get_absolute_path

NULL_TREE = git.NULL_TREE
_MARKER = object()
GIT_IGNORE = ".gitignore"


def git_unicode_unescape(s: Optional[str], encoding: str = "utf-8") -> str:
    """Undoes git/GitPython unicode encoding."""
    if s is None:
        return ""

    if s.startswith('"'):
        return s.strip('"').encode("latin1").decode("unicode-escape").encode("latin1").decode(encoding)
    return s


def split_paths(*paths):
    """Return a generator with split list of paths."""
    argument_batch_size = 100

    batch_count = math.ceil(len(paths) / argument_batch_size)
    batch_count = max(batch_count, 1)

    for index in range(batch_count):
        yield paths[index * argument_batch_size : (index + 1) * argument_batch_size]


class BaseRepository:
    """Abstract Base repository."""

    def __init__(self, path: Union[Path, str] = ".", repository: Optional[git.Repo] = None):
        super().__init__()

        self._repository: Optional[git.Repo] = repository
        self._path = Path(path).resolve()
        self._lfs: Optional["LFS"] = None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.path}>"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    @property
    def path(self) -> Path:
        """Absolute path to the repository's root."""
        return self._path

    @property
    def head(self) -> "SymbolicReference":
        """HEAD of the repository."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return SymbolicReference(self._repository, "HEAD")

    @property
    def active_branch(self) -> Optional["Branch"]:
        """Return current checked out branch."""
        if self.head.reference is None:
            return None
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return Branch(self._repository, self.head.reference.path)

    @property
    def branches(self) -> "BranchManager":
        """Return all branches."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return BranchManager(self._repository)

    @property
    def remotes(self) -> "RemoteManager":
        """Return all remotes."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return RemoteManager(self._repository)

    @property  # type: ignore
    @lru_cache()
    def submodules(self) -> "SubmoduleManager":
        """Return a list of submodules."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return SubmoduleManager(self._repository)

    @property
    def staged_changes(self) -> List["Diff"]:
        """Return a list of staged changes.

        NOTE: This can be implemented by ``git diff --cached --name-status -z``.
        """
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        try:
            diff = self._repository.index.diff("HEAD", ignore_submodules=True)
            return [Diff.from_diff(d) for d in diff]
        except (git.BadName, git.BadObject, git.GitError) as e:  # type: ignore
            raise errors.GitError("Cannot get staged changes") from e

    @property
    def tags(self) -> "TagManager":
        """Return all available tags."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return TagManager(self._repository)

    @property
    def unstaged_changes(self) -> List["Diff"]:
        """Return a list of changes that are not staged."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        try:
            diff = self._repository.index.diff(None, ignore_submodules=True)
            return [Diff.from_diff(d) for d in diff]
        except (git.BadName, git.BadObject, git.GitError) as e:  # type: ignore
            raise errors.GitError("Cannot get modified changes") from e

    @property
    def unmerged_blobs(self) -> Dict[str, List[Tuple[int, "Object"]]]:
        """Return a map of path to stage and blob for unmerged blobs in the current index."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return {
            str(path): [(e[0], Object.from_object(e[1])) for e in values]
            for path, values in self._repository.index.unmerged_blobs().items()
        }

    @property
    def untracked_files(self) -> List[str]:
        """Return the list of untracked files."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return self._repository.untracked_files

    @property
    def files(self) -> List[str]:
        """Return a list of all files in the current version of the repository."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return [str(e[0]) for e in self._repository.index.entries]

    @property
    def all_files(self) -> List[str]:
        """Return absolute paths of all files in the index and untracked files."""
        return [os.path.join(self.path, path) for path in itertools.chain(self.files, self.untracked_files)]

    @property
    def lfs(self) -> "LFS":
        """Return a Git LFS manager."""
        if self._lfs is None:
            self._lfs = LFS(self)

        return self._lfs

    def is_valid(self) -> bool:
        """Return True if a valid repository exists."""
        return self._repository is not None

    def get_historical_changes_patch(self, path: Union[Path, str]) -> List[str]:
        """Return a patch of all changes to a file."""
        return self.run_git_command("log", "--", str(path), patch=True, follow=True).splitlines()

    def add(self, *paths: Union[Path, str], force: bool = False, all: bool = False) -> None:
        """Add a list of files to be committed to the VCS."""
        if all:
            assert len(paths) == 0, "Cannot pass both ``all`` and ``paths``."
            self.run_git_command("add", all=True, force=force)
        else:
            for batch in split_paths(*paths):
                self.run_git_command("add", *batch, force=force)

    def add_ignored_pattern(self, pattern: str) -> None:
        """Add the pattern to the ``.gitignore`` file."""
        with open(self.path / GIT_IGNORE, "a+b") as file:
            file.seek(-1, os.SEEK_END)
            last_character = file.read(1)
            has_newline = last_character == b"\n"
            pattern = f"{pattern}{os.linesep}" if has_newline else f"{os.linesep}{pattern}{os.linesep}"
            file.write(pattern.encode("utf-8"))

    def commit(
        self,
        message: str,
        *,
        amend: bool = False,
        author: "Actor" = None,
        committer: "Actor" = None,
        no_verify: bool = False,
        no_edit: bool = False,
        paths: Optional[List[Union[Path, str]]] = None,
    ) -> "Commit":
        """Commit added files to the VCS."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        env = {}
        if author:
            env.update({"GIT_AUTHOR_NAME": author.name, "GIT_AUTHOR_EMAIL": author.email})
        if committer:
            env.update({"GIT_COMMITTER_NAME": committer.name, "GIT_COMMITTER_EMAIL": committer.email})

        # NOTE: Only commit specified paths
        args = ["--"] + [str(p) for p in paths] if paths else []

        self.run_git_command(
            "commit", *args, message=message, no_verify=no_verify, amend=amend, no_edit=no_edit, env=env
        )

        return Commit.from_commit(self._repository, self._repository.head.commit)

    def checkout(self, reference: Optional[Union["Branch", "Tag", str]] = None, sparse: Optional[List[Path]] = None):
        """Check-out a specific reference."""
        if sparse is not None:
            self.run_git_command("sparse-checkout", "init", "--cone")
            self.run_git_command("sparse-checkout", "set", *sparse)

        self.run_git_command("checkout", reference)

    def clean(self, paths: Optional[Sequence[Union[Path, str]]] = None):
        """Remove untracked files."""
        self.run_git_command("clean", "-xdff", paths)

    def fetch(
        self,
        remote: Union["Remote", str] = None,
        refspec: Union["Branch", str] = None,
        all: bool = False,
        tags: bool = False,
        unshallow: bool = False,
        depth: int = None,
    ):
        """Update a remote branches."""
        if all:
            assert remote is None and refspec is None, "Cannot fetch all while a reference is passed"

        self.run_git_command(
            "fetch", _to_string(remote), _to_string(refspec), all=all, unshallow=unshallow, depth=depth, tags=tags
        )

    def contains(self, path: Union[Path, str]) -> bool:
        """Return True if path is tracked in the repository."""
        try:
            self.run_git_command("ls-files", "--error-unmatch", str(path))
        except errors.GitCommandError:
            return False
        else:
            return True

    def move(self, *sources: Union[Path, str], destination: Union[Path, str], force: bool = False):
        """Move source files to the destination."""
        self.run_git_command("mv", *sources, destination, force=force)

    def pull(self, remote: Union["Remote", str] = None, refspec: Union["Branch", str] = None):
        """Update changes from remotes."""
        self.run_git_command("pull", _to_string(remote), _to_string(refspec))

    def push(
        self,
        remote: Union["Remote", str] = None,
        refspec: Union["Branch", str] = None,
        *,
        no_verify: bool = False,
        set_upstream: bool = False,
        delete: bool = False,
        force: bool = False,
    ):
        """Push local changes to a remote repository."""
        self.run_git_command(
            "push",
            _to_string(remote),
            _to_string(refspec),
            no_verify=no_verify,
            set_upstream=set_upstream,
            delete=delete,
            force=force,
        )

    def remove(
        self,
        *paths: Union[Path, str],
        index: bool = False,
        not_exists_ok: bool = False,
        recursive: bool = False,
        force: bool = False,
    ):
        """Remove paths from repository or index."""
        self.run_git_command("rm", "--", *paths, cached=index, ignore_unmatch=not_exists_ok, r=recursive, force=force)

    def reset(self, reference: Union["Branch", "Commit", "Reference", str] = None, hard: bool = False):
        """Reset a git repository to a given reference."""
        self.run_git_command("reset", _to_string(reference), hard=hard)

    def status(self) -> str:
        """Return status of a repository."""
        return self.run_git_command("status")

    def create_worktree(
        self,
        path: Path,
        reference: Optional[Union["Branch", "Commit", "Reference", str]],
        branch: Optional[str] = None,
        checkout: bool = True,
        detach: bool = False,
    ):
        """Create a git worktree.

        Args:
            path(Path): Target folder.
            reference(Union[Branch, Commit, Reference, str]): the reference to base the tree on.
            checkout(bool, optional): Whether to perform a checkout of the reference (Default value = False).
            detach(bool, optional): Whether to detach HEAD in worktree (Default value = False).
        """
        args = ["add"]

        # NOTE: pass args as string to ensure correct order
        if checkout:
            args.append("--checkout")
        if detach:
            args.append("--detach")

        if branch:
            args.extend(["-b", branch])

        args.append(str(path))

        if reference:
            args.append(str(reference))
        self.run_git_command("worktree", *args)

    def remove_worktree(self, path: Path):
        """Create a git worktree.

        Args:
            path(Path): Worktree folder.
        """
        self.run_git_command("worktree", "remove", path)

    def is_dirty(self, untracked_files: bool = False) -> bool:
        """Return True if the repository has modified or untracked files ignoring submodules."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return self._repository.is_dirty(untracked_files=untracked_files, submodules=False)

    def run_git_command(self, command: str, *args, **kwargs) -> str:
        """Run a git command in this repository."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")
        return _run_git_command(self._repository, command, *args, **kwargs)

    def get_attributes(self, *paths: Union[Path, str]) -> Dict[str, Dict[str, str]]:
        """Return a map from paths to its attributes.

        NOTE: Dict keys are the same relative or absolute path as inputs.
        """
        if len(paths) == 0:
            return {}

        attributes: Dict[str, Dict[str, str]] = defaultdict(dict)

        for batch in split_paths(*paths):
            data = self.run_git_command("check-attr", "-z", "--all", "--", *batch)
            for path, name, value in zip_longest(*[iter(data.strip("\0").split("\0"))] * 3):
                if path:
                    attributes[path][name] = value

        return attributes

    def get_previous_commit(
        self,
        path: Union[Path, str],
        revision: Optional[Union["Commit", str]] = None,
        first: bool = False,
        full_history: bool = True,
        submodule: bool = False,
    ) -> Optional["Commit"]:
        """Return a previous commit for a given path starting from ``revision``."""
        revision = revision or "HEAD"
        assert isinstance(revision, (Commit, str)), f"'revision' must be Commit/str not '{type(revision)}'"

        commit = _find_previous_commit_helper(
            repository=self,
            path=path,
            revision=str(revision),
            first=first,
            full_history=full_history,
            submodules=submodule,
        )
        if not commit:
            raise errors.GitCommitNotFoundError(f"Cannot find previous commit for '{path}' from '{revision}'")
        return commit

    def get_revisions_paths(self, *checksums: str) -> List[Tuple[Optional[str], Optional[str]]]:
        """Return a revision:path tuple for each checksum so that revision contains the given blob with the checksum."""

        def parse_output(output) -> Tuple[Optional[str], Optional[str]]:
            if output.lstrip().startswith("fatal:"):
                return None, None
            else:
                return output.split(":", maxsplit=1)

        outputs = []
        for batch in split_paths(*checksums):
            try:
                outputs.extend(self.run_git_command("describe", "--always", *batch).split(os.linesep))
            except errors.GitCommandError:
                outputs.extend(["fatal:"] * len(batch))

        return [parse_output(o) for o in outputs]

    def get_sizes(self, *checksums: str) -> List[Optional[str]]:
        """Return size of blobs given their checksum."""
        revisions = []
        for batch in split_paths(*checksums):
            try:
                revisions.extend(self.run_git_command("describe", "--always", *batch).split(os.linesep))
            except errors.GitCommandError:
                revisions.extend(["fatal:"] * len(batch))

        try:
            result = subprocess.run(
                ["git", "cat-file", "--batch-check='%(objectsize)'"],
                check=True,
                input="\n".join(revisions),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                cwd=self.path,
                text=True,
            )
        except subprocess.CalledProcessError:
            return [""] * len(checksums)
        else:
            return ["" if "missing" in line else line.strip("\"'") for line in result.stdout.split(os.linesep)[:-1]]

    def iterate_commits(
        self,
        *paths: Union[Path, str],
        revision: Optional[str] = None,
        reverse: bool = False,
        full_history: bool = False,
        max_count: int = -1,
    ) -> Generator["Commit", None, None]:
        """Return a list of commits."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        revision = revision or "HEAD"
        assert isinstance(revision, str), f"'revision' must be string not '{type(revision)}'"

        try:
            for c in self._repository.iter_commits(
                rev=revision, paths=paths, reverse=reverse, full_history=full_history, max_count=max_count
            ):
                yield Commit.from_commit(self._repository, c)
        except git.GitCommandError:
            return

    def get_commit(self, revision: str) -> "Commit":
        """Return Commit with the provided sha."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        try:
            commit = self._repository.commit(revision)
        except (ValueError, IndexError, git.BadName, git.BadObject) as e:  # type: ignore
            raise errors.GitCommitNotFoundError(f"Cannot find a commit with revision '{revision}'") from e
        else:
            wrapped_commit = Commit.from_commit(self._repository, commit)
            if not wrapped_commit:
                raise errors.GitCommitNotFoundError(f"Cannot find a commit with revision '{revision}'")
            return wrapped_commit

    def get_ignored_paths(self, *paths: Union[Path, str]) -> List[str]:
        """Return ignored paths matching ``.gitignore`` file.

        NOTE: This function returns the same value as inputs: If input is an absolute path output is an absolute path.
        The same is true for relative paths.
        NOTE: Relative paths should be relative to the current working directory and not the repository's root.
        """
        ignored = []

        for batch in split_paths(*paths):
            try:
                ignored.extend(self.run_git_command("check-ignore", *batch).split(os.linesep))
            except errors.GitCommandError:
                pass

        return ignored

    def get_content(
        self,
        path: Union[Path, str],
        *,
        revision: Optional[Union["Reference", str]] = None,
        checksum: Optional[str] = None,
        binary: bool = False,
    ) -> Union[bytes, str]:
        """Get content of a file in a given revision as text or binary."""
        output = self.copy_content_to_file(path=path, checksum=checksum, revision=revision)
        output_path = Path(output)

        content: Union[bytes, str] = output_path.read_bytes() if binary else output_path.read_text()
        delete_dataset_file(output)

        return content

    def get_raw_content(
        self, *, path: Union[Path, str], revision: Optional[str] = None, checksum: Optional[str] = None
    ) -> str:
        """Get raw content of a file in a given revision as text without applying any filter on it."""
        output = self.copy_content_to_file(path=path, checksum=checksum, revision=revision, apply_filters=False)

        return Path(output).read_text()

    def copy_content_to_file(
        self,
        path: Union[Path, str],
        *,
        revision: Optional[Union["Reference", str]] = None,
        checksum: Optional[str] = None,
        output_path: Optional[Union[Path, str]] = None,
        apply_filters: bool = True,
    ) -> str:
        """Get content of an object using its checksum, write it to a file, and return the file's path.

        Args:
            path(Union[Path, str]): Relative or absolute path to the file.
            revision(Optional[Union[Reference, str]]): A commit/branch/tag to get the file from. This cannot be passed
                with ``checksum``.
            checksum(Optional[str]): Git hash of the file to be retrieved. This cannot be passed with ``revision``.
            output_path(Optional[Union[Path, str]]): A path to copy the content to. A temporary file is created if it is
                ``None``.
            apply_filters(bool): Whether to apply Git filter on the retrieved object. Note that ``apply_filters`` still
                works if repository is cloned with ``--skip-smudge`` or if ``GIT_LFS_SKIP_SMUDGE`` is set. It also works
                if there is not entry for the file in ``.gitattributes`` (e.g. when a file was deleted). The reason is
                that we use `git lfs smudge` command to get the file content if this option is passed and we also
                disable ``GIT_LFS_SKIP_SMUDGE``.

        Returns:
            The path to the created file.

        """
        absolute_path = get_absolute_path(path, self.path)

        def get_content_helper(output_file) -> bool:
            command = ["git", "cat-file"]

            if checksum is None:
                assert revision is not None, "Either ``revision`` or ``checksum`` must be passed."
                relative_path = os.path.relpath(absolute_path, self.path)
                prefix = "--filters" if apply_filters else "-p"
                object = [prefix, f"{revision}:{relative_path}"]
            else:
                assert revision is None, "Cannot pass both ``revision`` and ``checksum``."
                object = ["--filters", "--path", absolute_path] if apply_filters else ["-p"]
                object.append(checksum)

            command.extend(object)
            try:
                subprocess.run(
                    command,
                    check=True,
                    stdout=output_file,
                    stderr=subprocess.DEVNULL,
                    cwd=self.path,
                )
            except subprocess.CalledProcessError:
                return False
            else:
                # NOTE: No need to check the exit code since ``check=True`` takes care of non-zero exit codes
                return True

        def get_content_from_submodules():
            for submodule in self.submodules:  # type: ignore[attr-defined]
                try:
                    Path(absolute_path).relative_to(submodule.path)
                except ValueError:
                    continue
                else:
                    return submodule.copy_content_to_file(
                        path=absolute_path, checksum=checksum, revision=revision, apply_filters=apply_filters
                    )

        output = None

        if output_path is None:
            with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as temp_output_file:
                if get_content_helper(output_file=temp_output_file):
                    output = temp_output_file.name
        else:
            with open(output_path, "wb") as output_file:
                if get_content_helper(output_file):
                    output = output_file.name

        if output is not None:
            if not apply_filters or not self.lfs.is_pointer_file(output):
                return output

            with tempfile.NamedTemporaryFile(mode="w+t", delete=False) as pointer:
                pointer.write(Path(output).read_text())

            self.lfs.get_content(pointer.name, output)
            return output

        from_submodules = get_content_from_submodules()
        if from_submodules:
            return from_submodules

        raise errors.FileNotFound(path, checksum=checksum, revision=revision)

    def get_object_hashes(
        self, paths: List[Union[Path, str]], revision: Optional[str] = None
    ) -> Dict[Union[Path, str], Optional[str]]:
        """Return git hash of an object in a Repo or its submodule.

        NOTE: path must be relative to the repo's root regardless if this function is called from a subdirectory or not.
        """

        def _get_uncommitted_file_hashes(paths: Set[Union[Path, str]]) -> Dict[str, str]:
            """Get hashes for all modified/uncommitted/staged files."""
            staged_files = [d.a_path for d in self.staged_changes] if self.head.is_valid() else []
            modified_files = [item.b_path for item in self.unstaged_changes if not item.deleted]
            dirty_files = {os.path.join(self.path, p) for p in self.untracked_files + modified_files + staged_files}
            dirty_files = {p for p in dirty_files if p in paths and not os.path.isdir(p)}
            dirty_files_list = list(dirty_files)

            dirty_files_hashes = Repository.hash_objects(cast(List[Union[Path, str]], dirty_files_list))
            return dict(zip(dirty_files_list, dirty_files_hashes))

        def _get_hashes_from_revision(
            paths: Set[Union[Path, str]], revision: str, repository: BaseRepository
        ) -> Dict[Union[Path, str], Optional[str]]:
            """Get hashes for paths in a specific revision."""
            existing_paths = repository.get_existing_paths_in_revision(paths, revision=revision)
            result: Dict[Union[Path, str], Optional[str]] = {}
            for batch in split_paths(*existing_paths):
                hashes = self.run_git_command("rev-parse", *[f"{revision}:{relative_path}" for relative_path in batch])
                result.update(zip(batch, hashes.splitlines()))

            for path in paths:
                if path not in result:
                    result[path] = None

            return result

        path_mapping = {get_absolute_path(path, self.path): path for path in paths}
        absolute_paths = set(path_mapping.keys())

        hashes: Dict[Union[Path, str], Optional[str]] = {}
        # NOTE: If revision is not specified, we use hash-object to hash the (possibly) modified object
        if not revision:
            uncommitted_hashes = _get_uncommitted_file_hashes(cast(Set[Union[Path, str]], absolute_paths))

            hashes.update({path_mapping.get(p, p): h for p, h in uncommitted_hashes.items()})

            if len(hashes) == len(absolute_paths):
                # NOTE: there were only uncommitted files
                return hashes

            revision = "HEAD"
            absolute_paths = {p for p in absolute_paths if p not in uncommitted_hashes}

        submodule_paths = defaultdict(list)
        main_repo_paths = []

        if len(self.submodules) > 0:  # type: ignore
            # NOTE: filter paths belonging to main repo from those belonging to submodules
            for absolute_path in absolute_paths:
                found = False
                for submodule in self.submodules:  # type: ignore
                    try:
                        Path(absolute_path).relative_to(submodule.path)
                        submodule_paths[submodule].append(absolute_path)
                        found = True
                        break
                    except ValueError:
                        continue

                if not found:
                    main_repo_paths.append(os.path.relpath(absolute_path, start=self.path))
        else:
            main_repo_paths = list(map(lambda p: os.path.relpath(p, start=self.path), absolute_paths))

        if main_repo_paths:
            # NOTE: Get hashes for paths in the main repository
            revision_hashes = _get_hashes_from_revision(set(main_repo_paths), revision, self)
            hashes.update({path_mapping.get(get_absolute_path(p, self.path), p): h for p, h in revision_hashes.items()})

        if not submodule_paths:
            return hashes

        # NOTE: Get hashes for paths in submodules
        for submodule, submodule_path in submodule_paths.items():
            submodule_hashes = submodule.get_object_hashes(paths=submodule_path, revision="HEAD")
            hashes.update(
                {path_mapping.get(get_absolute_path(p, self.path), p): h for p, h in submodule_hashes.items()}
            )

        return hashes

    def get_object_hash(self, path: Union[Path, str], revision: Optional[Union["Commit", str]] = None) -> Optional[str]:
        """Return git hash of an object in a Repo or its submodule.

        NOTE: path must be relative to the repo's root regardless if this function is called from a subdirectory or not.
        """
        absolute_path = get_absolute_path(path, self.path)
        had_revision = bool(revision)

        # NOTE: If revision is not specified, we use hash-object to hash the (possibly) modified object
        if not revision:
            try:
                return Repository.hash_object(absolute_path)
            except errors.GitCommandError:
                # NOTE: If object does not exist anymore, hash-object doesn't work, fall back to rev-parse
                revision = "HEAD"

        def get_staged_directory_hash() -> Optional[str]:
            if not os.path.isdir(absolute_path):
                return None

            try:
                # NOTE: Stage the path. If the path is not staged then it won't be found in ``stashed_revision``
                if not self.staged_changes:
                    self.add(relative_path, force=True)

                stashed_revision = self.run_git_command("stash", "create")
                if not stashed_revision:
                    return None

                return self.run_git_command("rev-parse", f"{stashed_revision}:{relative_path}")
            except errors.GitCommandError:
                return None

        def get_object_hash_from_submodules() -> Optional[str]:
            for submodule in self.submodules:  # type: ignore
                try:
                    Path(absolute_path).relative_to(submodule.path)
                except ValueError:
                    continue
                else:
                    return submodule.get_object_hash(path=absolute_path, revision="HEAD")

            return None

        relative_path = os.path.relpath(absolute_path, start=self.path)

        try:
            return self.run_git_command("rev-parse", f"{revision}:{relative_path}")
        except errors.GitCommandError:
            # NOTE: If a revision was specified then don't look in the staging area
            if not had_revision:
                # NOTE: The file can be in a submodule or it can be a directory which is staged but not committed yet.
                # It's also possible that the file was not there when the command ran but was there when workflows were
                # migrated (this can happen only for Usage); the project might be broken too.
                staged_directory_hash = get_staged_directory_hash()
                if staged_directory_hash:
                    return staged_directory_hash

            return get_object_hash_from_submodules()

    def get_user(self) -> "Actor":
        """Return the local/global git user."""
        configuration = self.get_configuration()
        return Repository._get_user_from_configuration(configuration)

    def close(self) -> None:
        """Close the underlying repository.

        Cleans up dangling processes.
        """
        if getattr(self, "_repository", None) is not None:
            try:
                self._repository.close()  # type:ignore
            except AttributeError:
                pass
            del self._repository
            self._repository = None

    @staticmethod
    def get_global_user() -> "Actor":
        """Return the global git user."""
        configuration = Repository.get_global_configuration()
        return Repository._get_user_from_configuration(configuration)

    @staticmethod
    def _get_user_from_configuration(configuration: "Configuration") -> "Actor":
        try:
            name = configuration.get_value("user", "name", None)
            email = configuration.get_value("user", "email", None)
        except errors.GitConfigurationError:  # pragma: no cover
            raise errors.GitConfigurationError(
                'The user name and email are not configured. Please use the "git config" command to configure them.\n\n'
                '\tgit config --global --add user.name "John Doe"\n'
                '\tgit config --global --add user.email "john.doe@example.com"\n'
            )

        # Check the git configuration.
        if not name:  # pragma: no cover
            raise errors.GitMissingUsername
        if not email:  # pragma: no cover
            raise errors.GitMissingEmail

        name = _sanitize_git_config_value(name)
        email = _sanitize_git_config_value(email)

        return Actor(name=name, email=email)

    def get_configuration(self, writable=False, scope: str = None) -> "Configuration":
        """Return git configuration.

        NOTE: Scope can be "global" or "local".
        """
        return Configuration(repository=self._repository, scope=scope, writable=writable)

    @staticmethod
    def get_global_configuration(writable: bool = False) -> "Configuration":
        """Return global git configuration."""
        return Configuration(repository=None, writable=writable)

    def get_existing_paths_in_revision(
        self, paths: Union[List[Union[Path, str]], Set[Union[Path, str]]] = None, revision: str = "HEAD"
    ) -> List[str]:
        """List all paths that exist in a revision."""

        try:
            if paths:
                dirs = []
                files = []

                for path in paths:
                    if os.path.isdir(path):
                        dirs.append(path)
                    else:
                        files.append(path)
                result = []
                if files:
                    # NOTE: check existing files
                    for batch in split_paths(*files):
                        existing_paths = git.Git(working_dir=self.path).ls_tree(*batch, r=revision, name_only=True)
                        result.extend(existing_paths.splitlines())

                if dirs:
                    # NOTE: check existing dirs
                    for batch in split_paths(*dirs):
                        existing_paths = git.Git(working_dir=self.path).ls_tree(
                            *batch, d=True, r=revision, name_only=True
                        )
                        result.extend(existing_paths.splitlines())

                return result
            else:
                existing_files = git.Git().ls_tree(r=revision, name_only=True).splitlines()
                existing_dirs = git.Git().ls_tree(r=revision, name_only=True, d=True).splitlines()
                return existing_dirs + existing_files
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e

    @staticmethod
    def hash_objects(paths: List[Union[Path, str]]) -> List[str]:
        """Create a git hash for a list of paths. The paths don't need to be in a repository."""
        hashes = []
        try:
            for batch in split_paths(*paths):
                calculated_hashes = git.Git().hash_object(*batch)
                hashes.extend(calculated_hashes.splitlines())
            return hashes
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e

    @staticmethod
    def hash_object(path: Union[Path, str]) -> Optional[str]:
        """Create a git hash for a a path. The path doesn't need to be in a repository."""
        result = BaseRepository.hash_objects([path])

        if result and len(result) > 0:
            return result[0]
        return None

    @staticmethod
    def hash_string(content: str) -> str:
        """Calculate the object-hash for a blob with specified content."""
        content_bytes = content.encode("utf-8")
        data = f"blob {len(content_bytes)}\0".encode("utf-8") + content_bytes
        return hashlib.sha1(data).hexdigest()


class Repository(BaseRepository):
    """Abstract Base repository."""

    def __init__(
        self, path: Union[Path, str] = ".", search_parent_directories: bool = False, repository: git.Repo = None
    ):
        repo = repository or _create_repository(path, search_parent_directories)

        super().__init__(path=Path(repo.working_dir).resolve(), repository=repo)  # type: ignore

    @classmethod
    def clone_from(
        cls,
        url: Union[Path, str],
        path: Union[Path, str],
        *,
        branch: Optional[str] = None,
        recursive: bool = False,
        depth: Optional[int] = None,
        progress: Optional[Callable] = None,
        no_checkout: bool = False,
        env: Optional[dict] = None,
        clone_options: Optional[List[str]] = None,
    ) -> "Repository":
        """Clone a remote repository and create an instance."""
        try:
            repository = git.Repo.clone_from(
                url=url,
                to_path=path,
                branch=branch,
                recursive=recursive,
                depth=depth,
                progress=progress,
                no_checkout=no_checkout,
                env=env,
                multi_options=clone_options,
            )
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e
        else:
            return cls(path=path, repository=repository)

    @classmethod
    def initialize(cls, path: Union[Path, str], *, bare: bool = False, branch: str = None) -> "Repository":
        """Initialize a git repository."""
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            repository = git.Repo.init(path=str(path), bare=bare, initial_branch=branch)
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e
        except git.GitError as e:
            raise errors.GitError(f"Git command failed: {str(e)}") from e
        else:
            return cls(path=path, repository=repository)


class LFS:
    """Git LFS manager."""

    def __init__(self, repository: BaseRepository):
        self._repository: BaseRepository = repository

    def install(self, skip_smudge: bool = True):
        """Force install Git LFS in the repository."""
        os.environ["GIT_LFS_SKIP_SMUDGE"] = "1" if skip_smudge else "0"

        command = ["lfs", "install", "--local", "--force"]
        if skip_smudge:
            command += ["--skip-smudge"]

        try:
            self._repository.run_git_command(*command)
        except errors.GitCommandError as e:
            raise errors.GitError(f"Cannot install Git LFS in {self}") from e

    def is_pointer_file(self, path: Union[Path, str]):
        """Check if a file is an LFS pointer."""
        try:
            self._repository.run_git_command("lfs", "pointer", check=True, file=path)
        except errors.GitError:
            return False
        else:
            return True

    def get_content(self, path: Union[Path, str], output_path: Union[Path, str]):
        """Get content from a given pointer file."""
        try:
            with open(path) as pointer_file, open(output_path, "wb") as output_file:
                # NOTE: This return the pointer_file content if it's not an LFS pointer file or doesn't point to a valid
                # LFS object
                subprocess.run(
                    ["git", "lfs", "smudge"],
                    check=True,
                    stdin=pointer_file,
                    stdout=output_file,
                    stderr=subprocess.DEVNULL,
                    cwd=self._repository.path,
                    env={"GIT_LFS_SKIP_SMUDGE": "0"},
                )
        except subprocess.CalledProcessError:
            return False
        else:
            # NOTE: No need to check the exit code since ``check=True`` takes care of non-zero exit codes
            return True


class Submodule(BaseRepository):
    """A git submodule."""

    def __init__(self, parent: git.Repo, name: str, path: Union[Path, str], url: str):
        super().__init__(path=path, repository=None)
        self._parent: git.Repo = parent or _create_repository(path=path, search_parent_directories=True)
        self._name: str = name
        self._url: str = url
        try:
            self._repository: Optional[git.Repo] = _create_repository(path, search_parent_directories=False)
        except errors.GitError:
            # NOTE: Submodule directory doesn't exist yet, so, we ignore the error
            pass

    @classmethod
    def from_submodule(cls, parent: git.Repo, submodule: git.Submodule) -> "Submodule":  # type: ignore
        """Create an instance from a git submodule."""
        path = Path(parent.working_dir) / submodule.path  # type: ignore
        return cls(parent=parent, name=submodule.name, path=path, url=submodule.url)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<Submodule {self.relative_path}>"

    def __del__(self) -> None:
        if getattr(self, "_repository", None) is not None:
            try:
                self._repository.close()  # type:ignore
            except AttributeError:
                pass
            del self._repository
            self._repository = None

    @property
    def name(self) -> str:
        """Return submodule's name."""
        return self._name

    @property
    def relative_path(self) -> Path:
        """Relative submodule's path to its parent repository."""
        return self._path.relative_to(self._parent.working_dir)  # type: ignore

    @property
    def url(self) -> str:
        """Return submodule's url."""
        return self._url


class SubmoduleManager:
    """Manage submodules of a Repository."""

    def __init__(self, repository: git.Repo):
        self._repository: Optional[git.Repo] = repository
        self._submodule_cache: Dict[str, Submodule] = {}  # type: ignore
        try:
            self.update()
        except errors.GitError:
            # NOTE: Update fails if submodule repo cannot be cloned. Repository still works but submodules are broken.
            pass

    def _get_submodule(self, submodule: git.Submodule) -> Submodule:  # type: ignore
        """Get a submodule from local cache."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        if submodule.name not in self._submodule_cache:
            submodule_result = Submodule.from_submodule(self._repository, submodule)
            self._submodule_cache[submodule.name] = submodule_result
        return self._submodule_cache[submodule.name]

    def __getitem__(self, name: str) -> Submodule:
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        try:
            submodule = self._repository.submodules[name]
        except IndexError:
            raise errors.GitError(f"Submodule '{name}' not found")
        else:
            return self._get_submodule(submodule)

    def __iter__(self):
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        for s in self._repository.submodules:

            yield self._get_submodule(s)

    def __len__(self) -> int:
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        return len(self._repository.submodules)

    def __repr__(self) -> str:
        return str(list(self))

    def remove(self, submodule: Union[Submodule, str], force: bool = False):
        """Remove an existing submodule."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        name = submodule if isinstance(submodule, str) else submodule.name

        try:
            git_submodule = self._repository.submodules[name]
            git_submodule.remove(force=force)

            if name in self._submodule_cache:
                submodule = self._submodule_cache[name]
                del self._submodule_cache[name]
                submodule.close()
        except git.GitError as e:
            raise errors.GitError(f"Cannot delete submodule '{submodule}'") from e

    def update(self, initialize: bool = True):
        """Update all submodule."""
        if self._repository is None:
            raise errors.ParameterError("Repository not set.")

        # NOTE: Git complains if ``--init`` comes before ``update``
        args = ("update", "--init") if initialize else ("update",)
        _run_git_command(self._repository, "submodule", *args)


class Object(NamedTuple):
    """Represent a git object."""

    path: str
    type: str
    size: int
    hexsha: str

    @classmethod
    def from_object(cls, object: git.Object):  # type: ignore
        """Create an instance from a git object."""
        return cls(path=object.path, type=object.type, size=object.size, hexsha=object.hexsha)


class Actor(NamedTuple):
    """Author/creator of a commit."""

    name: str
    email: str

    def __eq__(self, other: Any) -> bool:
        return self.name == other.name and self.email == other.email

    def __hash__(self) -> int:
        return hash((self.name, self.email))


class Diff(NamedTuple):
    """A single diff object between two trees."""

    # NOTE: In case a rename, a_path and b_path have different values. Make sure to use the correct one.
    a_path: str
    b_path: str
    """
    Possible values:
        A = Added
        D = Deleted
        R = Renamed
        M = Modified
        T = Changed in the type
    """
    change_type: str

    @classmethod
    def from_diff(cls, diff: git.Diff):
        """Create an instance from a git object."""
        a_path = git_unicode_unescape(diff.a_path)
        b_path = git_unicode_unescape(diff.b_path)

        # NOTE: Make sure a_path or b_path are the same in case of addition or deletion
        a_path = a_path or b_path
        b_path = b_path or a_path

        return cls(a_path=a_path, b_path=b_path, change_type=cast(str, diff.change_type))

    @property
    def deleted(self) -> bool:
        """True if file was deleted."""
        return self.change_type == "D"

    @property
    def added(self) -> bool:
        """True if file was added."""
        return self.change_type == "A"


class Commit:
    """A VCS commit."""

    def __init__(self, repository: git.Repo, commit: git.Commit):  # type: ignore
        self._repository: git.Repo = repository
        self._commit: git.Commit = commit  # type: ignore
        self._hexsha: str = commit.hexsha
        self._author: Actor = Actor(name=commit.author.name, email=commit.author.email)
        self._committer: Actor = Actor(name=commit.committer.name, email=commit.committer.email)

    @classmethod
    def from_commit(
        cls, repository: git.Repo, commit: Union[git.Commit, git.types.Commit_ish]  # type: ignore
    ) -> "Commit":
        """Create an instance from a git Commit object."""
        return Commit(repository, commit)

    def __eq__(self, other: Any) -> bool:
        return self._hexsha == getattr(other, "_hexsha", _MARKER)

    def __hash__(self) -> int:
        return hash(self._hexsha)

    def __str__(self) -> str:
        return self.hexsha

    def __repr__(self) -> str:
        return f"<Commit {self.hexsha}>"

    @property
    def hexsha(self) -> str:
        """Commit sha."""
        return self._hexsha

    @property
    def message(self) -> str:
        """Commit message."""
        return self._commit.message

    @property
    def author(self) -> Actor:
        """Author of the commit."""
        return self._author

    @property
    def committer(self) -> Actor:
        """Committer of the commit."""
        return self._committer

    @property
    def committed_datetime(self) -> datetime:
        """Commit date."""
        return self._commit.committed_datetime

    @property
    def authored_datetime(self) -> datetime:
        """Commit authored date."""
        return self._commit.authored_datetime

    @property
    def parents(self) -> List["Commit"]:
        """List of commit parents."""
        result = []

        for p in self._commit.parents:
            result.append(Commit.from_commit(self._repository, p))
        return result

    @property
    def tree(self) -> Dict[str, Object]:
        """Return all objects in the commit's tree."""
        return {o.path: Object.from_object(o) for o in self._commit.tree.traverse()}

    def get_changes(
        self, paths: Union[Path, str, List[Union[Path, str]], None] = None, commit: Union[str, "Commit"] = None
    ) -> List[Diff]:
        """Return list of changes in a commit.

        NOTE: This function can be implemented with ``git diff-tree``.
        """
        if commit:
            if isinstance(commit, Commit):
                commit = commit.hexsha

            diff = self._commit.diff(commit, paths=paths, ignore_submodules=True)
        elif len(self._commit.parents) == 0:
            diff = self._commit.diff(git.NULL_TREE, paths=paths, ignore_submodules=True)
        elif len(self._commit.parents) == 1:
            # NOTE: Diff is reverse so we get the diff of the parent to the child
            diff = self._commit.parents[0].diff(self._commit, paths=paths, ignore_submodules=True)
        else:
            # NOTE: A merge commit, so there is no clear diff
            return []

        return [Diff.from_diff(d) for d in diff]

    def traverse(self) -> Generator[Object, None, None]:
        """Traverse over all objects that are present in this commit."""
        return (Object.from_object(o) for o in self._commit.tree.traverse())

    def compare_to(self, other: "Commit") -> int:
        """Return -1 if self is made before other."""
        if self._repository.is_ancestor(self, other):  # type: ignore
            return -1
        if self._repository.is_ancestor(other, self):  # type: ignore
            return 1

        if self._commit.committed_date < other._commit.committed_date:
            return -1
        if self._commit.committed_date > other._commit.committed_date:
            return 1

        if self._commit.authored_date < other._commit.authored_date:
            return -1
        if self._commit.authored_date > other._commit.authored_date:
            return 1

        # NOTE: There is no ordering between the commits
        return 0


T = TypeVar("T", bound="Reference")


class Reference:
    """A git reference."""

    def __init__(self, repository: git.Repo, path: str):
        self._repository = repository
        self._reference = git.Reference(repo=repository, path=path, check_path=False)

    @classmethod
    def from_reference(cls: Type[T], repository: git.Repo, reference: git.Reference) -> T:
        """Create an instance from a git reference."""
        return cls(repository, reference.path)

    def __eq__(self, other: Any) -> bool:
        return self.path == getattr(other, "path", _MARKER)

    def __hash__(self) -> int:
        return hash(self.path)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.path}>"

    @property
    def commit(self) -> Commit:
        """Commit pointed to by the reference."""
        try:
            return Commit.from_commit(self._repository, self._reference.commit)
        except ValueError as e:
            raise errors.GitError("Not a valid reference") from e

    @property
    def path(self) -> str:
        """Reference path."""
        return self._reference.path

    @property
    def name(self) -> str:
        """Reference name."""
        return self._reference.name

    def is_valid(self) -> bool:
        """Return True if the reference is valid."""
        return self._reference.is_valid()


class SymbolicReference(Reference):
    """A git symbolic reference."""

    @property
    def reference(self) -> Optional[Reference]:
        """Return the reference that this object points to."""
        try:
            return Reference.from_reference(repository=self._repository, reference=self._reference.reference)
        except (git.GitError, TypeError):
            return None


class RemoteReference(Reference):
    """A git remote reference."""

    @property
    def remote(self) -> "Remote":
        """Return reference's remote."""
        remote_name = self.path.split("/")[2]  # /refs/remotes/<remote name>/<branch name>
        return Remote(self._repository, remote_name)


class Branch(Reference):
    """A git branch."""

    def __init__(self, repository: git.Repo, path: str):
        super().__init__(repository, path)
        self._reference = git.Head(repo=repository, path=path, check_path=False)

    @classmethod
    def from_head(cls, repository: git.Repo, head: git.Head):
        """Create an instance from a git.Head."""
        return cls(repository, head.path)

    @property
    def remote_branch(self) -> Optional[RemoteReference]:
        """Return the remote branch if any."""
        remote_reference = self._reference.tracking_branch()
        if remote_reference:
            return RemoteReference.from_reference(repository=self._repository, reference=remote_reference)

        return None


class BranchManager:
    """Manage branches of a Repository."""

    def __init__(self, repository: git.Repo):
        self._repository = repository

    def __getitem__(self, name_or_index: Union[int, str]) -> Branch:
        try:
            branch = self._repository.heads[name_or_index]
        except IndexError:
            raise errors.GitReferenceNotFoundError(f"Branch '{name_or_index}' not found")
        else:
            return Branch.from_head(self._repository, branch)

    def __iter__(self):
        return (Branch.from_head(self._repository, h) for h in self._repository.heads)

    def __len__(self) -> int:
        return len(self._repository.heads)

    def __repr__(self) -> str:
        return str(list(self))

    def add(self, name: str) -> Branch:
        """Add a new branch."""
        try:
            head = self._repository.create_head(path=name)
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e
        else:
            return Branch.from_head(repository=self._repository, head=head)

    def remove(self, branch: Union[Branch, str], force: bool = False):
        """Remove an existing branch."""
        _run_git_command(self._repository, "branch", branch, delete=True, force=force)


class Remote:
    """Remote of a Repository."""

    def __init__(self, repository: git.Repo, name: str):
        self._repository = repository
        self._remote = git.Remote(repo=repository, name=name)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<Remote {self.name}>"

    @classmethod
    def from_remote(cls, repository: git.Repo, remote: git.Remote) -> "Remote":
        """Create an instance from a git remote."""
        return cls(repository, remote.name)

    @property
    def references(self) -> List["RemoteReference"]:
        """Return a list of remote references."""
        return [RemoteReference.from_reference(self._repository, r) for r in self._remote.refs]

    @property
    def name(self) -> str:
        """Remote's  name."""
        return self._remote.name

    @property
    def url(self) -> Optional[str]:
        """Remote's  URL."""
        try:
            return self._remote.url
        except git.GitError:
            return None

    def is_valid(self) -> bool:
        """Return True if remote exists."""
        return bool(self._remote.exists())

    def set_url(self, url: str):
        """Change URL of a remote."""
        _run_git_command(self._repository, "remote", "set-url", self.name, url)

    @property
    def head(self) -> str:
        """The head commit of the remote."""
        self._remote.fetch()
        return _run_git_command(self._repository, "rev-parse", f"{self._remote.name}/{self._repository.active_branch}")


class RemoteManager:
    """Manage remotes of a Repository."""

    def __init__(self, repository: git.Repo):
        self._repository = repository

    def __getitem__(self, name_or_index: Union[int, str]) -> Remote:
        try:
            remote = self._repository.remotes[name_or_index]
        except IndexError:
            raise errors.GitRemoteNotFoundError(f"Remote '{name_or_index}' not found")
        else:
            return Remote.from_remote(repository=self._repository, remote=remote)

    def __iter__(self):
        return (Remote.from_remote(self._repository, r) for r in self._repository.remotes)

    def __len__(self) -> int:
        return len(self._repository.remotes)

    def __repr__(self) -> str:
        return str(list(self))

    def add(self, name: str, url: str) -> Remote:
        """Add a new remote."""
        try:
            remote = self._repository.create_remote(name=name, url=url)
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e
        else:
            return Remote.from_remote(self._repository, remote)

    def remove(self, remote: Union[Remote, str]):
        """Remove an existing remote."""
        try:
            self._repository.delete_remote(remote=remote)  # type: ignore
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e

    def get(self, remote: str) -> Optional[Remote]:
        """Return the given remote if it exists."""
        try:
            return self[remote]
        except errors.GitRemoteNotFoundError:
            return None


class Tag(Reference):
    """A git tag."""

    def __init__(self, repository: git.Repo, path: str):
        super().__init__(repository, path)
        self._reference = git.Tag(repo=repository, path=path, check_path=False)

    @classmethod
    def from_tag(cls, repository: git.Repo, tag: git.Tag):
        """Create an instance from a git.Head."""
        return cls(repository, tag.path)

    @property
    def commit(self) -> Commit:
        """Return the commit the tag refers to."""
        try:
            commit = self._reference.commit
        except ValueError:
            commit = self._reference.object

        if commit is None:
            raise errors.GitError(f"Not a valid reference: {self._reference.object}")

        return Commit.from_commit(self._repository, commit)


class TagManager:
    """Manage tags of a Repository."""

    def __init__(self, repository: git.Repo):
        self._repository = repository

    def __getitem__(self, name_or_index: Union[int, str]) -> Tag:
        try:
            tag = self._repository.tags[name_or_index]
        except IndexError:
            raise errors.GitReferenceNotFoundError(f"Tag '{name_or_index}' not found")
        else:
            return Tag.from_tag(self._repository, tag)

    def __iter__(self):
        return (Tag.from_tag(self._repository, t) for t in self._repository.tags)

    def __len__(self) -> int:
        return len(self._repository.tags)

    def __repr__(self) -> str:
        return str(list(self))

    def add(self, name: str) -> Tag:
        """Add a new tag."""
        try:
            tag = self._repository.create_tag(path=name)
        except git.GitCommandError as e:
            raise errors.GitCommandError(
                message=f"Git command failed: {str(e)}",
                command=e.command,
                stdout=e.stdout,
                stderr=e.stderr,
                status=e.status,
            ) from e
        else:
            return Tag.from_tag(repository=self._repository, tag=tag)

    def remove(self, tag: Union[Tag, str]):
        """Remove an existing tag."""
        _run_git_command(self._repository, "tag", tag, delete=True)


class Configuration:
    """Git configuration manager."""

    def __init__(self, repository: git.Repo = None, scope: str = None, writable: bool = True):
        assert scope is None or scope in ("global", "local"), f"Invalid scope: '{scope}'"

        self._read_only = not writable

        if scope == "local":
            scope = "repository"

        if repository is None:
            assert scope is None or scope == "global", "Scope can only be global"
            config_file = git.config.get_config_path("global")
            self._configuration = git.GitConfigParser(
                config_level="global", read_only=not writable, file_or_files=config_file, repo=None
            )
        elif writable:
            # NOTE: By default, we update only repository's configuration
            scope = scope or "repository"
            self._configuration = repository.config_writer(config_level=scope)  # type: ignore
        else:
            self._configuration = repository.config_reader(config_level=scope)  # type: ignore

    def __enter__(self) -> "Configuration":
        self._configuration.__enter__()
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        self._configuration.release()

    def get_value(self, section: str, option: str, default: Any = None) -> Any:
        """Return a config value."""
        try:
            return self._configuration.get_value(section=section, option=option, default=default)
        except configparser.Error as e:
            raise errors.GitConfigurationError(f"Cannot get value '{section} {option}'") from e

    def set_value(self, section: str, option: str, value: Any = None) -> Any:
        """Set a config value."""
        if self._read_only:
            raise errors.GitConfigurationError("Cannot write to a read-only configuration.")
        try:
            return self._configuration.set_value(section=section, option=option, value=value)
        except configparser.Error as e:
            raise errors.GitConfigurationError(f"Cannot set value '{section} {option}'") from e

    def remove_value(self, section: str, option: str):
        """Remove a config entry."""
        if self._read_only:
            raise errors.GitConfigurationError("Cannot write to a read-only configuration.")
        try:
            return self._configuration.remove_option(section=section, option=option)
        except configparser.Error as e:
            raise errors.GitConfigurationError(f"Cannot remove value '{section} {option}'") from e

    def has_section(self, section: str) -> bool:
        """Return if config file has a section."""
        return self._configuration.has_section(section)


def _create_repository(path: Union[Path, str], search_parent_directories: bool = False) -> git.Repo:
    """Create a git Repository."""
    try:
        if os.path.isdir(path):
            Path(path).mkdir(parents=True, exist_ok=True)
        return git.Repo(path, search_parent_directories=search_parent_directories)
    except (git.InvalidGitRepositoryError, git.NoSuchPathError) as e:
        raise errors.GitError(f"Git command failed: {str(e)}") from e


def _run_git_command(repository: git.Repo, command: str, *args, **kwargs) -> str:
    """Run a git command in the repository."""
    try:
        return getattr(repository.git, command)(*args, **kwargs)
    except git.GitCommandError as e:
        raise errors.GitCommandError(
            message=f"Git command failed: {str(e)}",
            command=e.command,
            stdout=e.stdout,
            stderr=e.stderr,
            status=e.status,
        ) from e


def _to_string(value) -> Optional[str]:
    return str(value) if value else None


def _find_previous_commit_helper(
    repository: BaseRepository,
    path: Union[Path, str],
    revision: Optional[str] = None,
    first: bool = False,
    full_history: bool = False,
    submodules: bool = False,
) -> Optional[Commit]:
    """Return a previous commit for a given path starting from ``revision``.

    Args:
        repository (BaseRepository): The repository to search in.
        path (Union[Path, str]): relative path to the file.
        revision (str, optional): revision to start from, defaults to ``HEAD`` (Default value = None).
        first (bool, optional): show the first commit in the history (Default value = False).
        full_history (bool, optional): search full history (Default value = False).
        submodules (bool, optional): search in submodules if not found in the main repository (Default value = False).
    """
    absolute_path = get_absolute_path(path, repository.path)
    revision = revision or "HEAD"

    def get_previous_commit_from_submodules() -> Optional[Commit]:
        for submodule in repository.submodules:  # type: ignore
            try:
                Path(absolute_path).relative_to(submodule.path)
            except ValueError:
                continue
            else:
                commit = _find_previous_commit_helper(
                    repository=submodule,
                    path=absolute_path,
                    revision=revision,
                    first=first,
                    full_history=full_history,
                    submodules=submodules,
                )
                if commit:
                    return commit

        return None

    max_count = 1 if not first else -1

    commits = list(
        repository.iterate_commits(absolute_path, revision=revision, full_history=full_history, max_count=max_count)
    )

    if len(commits) > 0:
        return commits[-1 if first else 0]

    if submodules:
        return get_previous_commit_from_submodules()

    return None


def _sanitize_git_config_value(value: str) -> str:
    """Remove quotation marks and whitespaces surrounding a config value."""
    return value.strip(" \n\t\"'")
