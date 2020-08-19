# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Client for handling datasets."""

import concurrent.futures
import os
import re
import shutil
import tempfile
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from subprocess import PIPE, SubprocessError, run
from urllib import error, parse
from urllib.parse import ParseResult

import attr
import patoolib
import requests
from git import GitCommandError, GitError, Repo
from wcmatch import glob

from renku.core import errors
from renku.core.management.clone import clone
from renku.core.management.config import RENKU_HOME
from renku.core.management.migrate import is_project_unsupported, migrate
from renku.core.models.datasets import (
    Dataset,
    DatasetFile,
    DatasetTag,
    generate_dataset_file_url,
    is_dataset_name_valid,
)
from renku.core.models.git import GitURL
from renku.core.models.locals import with_reference
from renku.core.models.provenance.agents import Person
from renku.core.models.refs import LinkReference
from renku.core.utils.urls import remove_credentials


@attr.s
class DatasetsApiMixin(object):
    """Client for handling datasets."""

    DATASETS = "datasets"
    """Directory for storing dataset metadata in Renku."""

    POINTERS = "pointers"
    """Directory for storing external pointer files."""

    CACHE = "cache"
    """Directory to cache transient data."""

    @property
    def renku_datasets_path(self):
        """Return a ``Path`` instance of Renku dataset metadata folder."""
        return self.path / self.renku_home / self.DATASETS

    @property
    def renku_pointers_path(self):
        """Return a ``Path`` instance of Renku pointer files folder."""
        path = self.path / self.renku_home / self.POINTERS
        path.mkdir(exist_ok=True)
        return path

    def datasets_from_commit(self, commit=None):
        """Return datasets defined in a commit."""
        commit = commit or self.repo.head.commit

        try:
            datasets = commit.tree / self.renku_home / self.DATASETS
        except KeyError:
            return

        for tree in datasets:
            try:
                blob = tree / self.METADATA
            except KeyError:
                continue
            dataset = Dataset.from_yaml(self.path / Path(blob.path), client=self)
            dataset.commit = commit
            yield dataset

    @property
    def datasets(self):
        """Return mapping from path to dataset."""
        result = {}
        paths = self.renku_datasets_path.rglob(self.METADATA)
        for path in paths:
            result[path] = self.load_dataset_from_path(path)
        return result

    def load_dataset_from_path(self, path, commit=None):
        """Return a dataset from a given path."""
        path = Path(path)
        if not path.is_absolute():
            path = self.path / path
        return Dataset.from_yaml(path, client=self, commit=commit)

    def get_dataset_path(self, name):
        """Get dataset path from name."""
        path = self.renku_datasets_path / name / self.METADATA
        if not path.exists():
            try:
                path = LinkReference(client=self, name="datasets/" + name).reference
            except errors.ParameterError:
                return None

        return path

    def load_dataset(self, name=None):
        """Load dataset reference file."""
        if name:
            path = self.get_dataset_path(name)
            if path and path.exists():
                return self.load_dataset_from_path(path)

    @contextmanager
    def with_dataset(self, name=None, create=False):
        """Yield an editable metadata object for a dataset."""
        dataset = self.load_dataset(name=name)
        clean_up_required = False

        if dataset is None:
            if not create:
                raise errors.DatasetNotFound(name=name)

            clean_up_required = True
            dataset, path, dataset_ref = self.create_dataset(name=name)
        elif create:
            raise errors.DatasetExistsError('Dataset exists: "{}".'.format(name))
        dataset_path = self.path / self.data_dir / dataset.name
        dataset_path.mkdir(parents=True, exist_ok=True)

        try:
            yield dataset
        except Exception:
            # TODO use a general clean-up strategy
            # https://github.com/SwissDataScienceCenter/renku-python/issues/736
            if clean_up_required:
                dataset_ref.delete()
                shutil.rmtree(path.parent, ignore_errors=True)
            raise

        dataset.to_yaml()

    def create_dataset(
        self, name=None, title=None, description=None, creators=None, keywords=None,
    ):
        """Create a dataset."""
        if not name:
            raise errors.ParameterError("Dataset name must be provided.")

        if not is_dataset_name_valid(name):
            raise errors.ParameterError('Dataset name "{}" is not valid.'.format(name))

        if self.load_dataset(name=name):
            raise errors.DatasetExistsError('Dataset exists: "{}".'.format(name))

        if not title:
            title = name

        identifier = str(uuid.uuid4())

        path = self.renku_datasets_path / identifier / self.METADATA

        if path.exists():
            raise errors.DatasetExistsError("Dataset with reference {} exists".format(path))

        path.parent.mkdir(parents=True, exist_ok=True)

        if creators is None:
            creators = [Person.from_git(self.repo)]

        keywords = keywords or ()

        with with_reference(path):
            dataset = Dataset(
                client=self,
                identifier=identifier,
                name=name,
                title=title,
                description=description,
                creators=creators,
                keywords=keywords,
            )

        dataset_ref = LinkReference.create(client=self, name="datasets/" + name)

        dataset_ref.set_reference(path)
        dataset.path = Path(dataset.path).relative_to(self.path)
        dataset.to_yaml()

        return dataset, path, dataset_ref

    def add_data_to_dataset(
        self,
        dataset,
        urls,
        force=False,
        overwrite=False,
        sources=(),
        destination="",
        ref=None,
        external=False,
        extract=False,
        all_at_once=False,
        destination_names=None,
        progress=None,
    ):
        """Import the data into the data directory."""
        messages = []
        warning_messages = []
        dataset_datadir = self.path / dataset.data_dir

        destination = destination or Path(".")
        destination = self._resolve_path(dataset_datadir, destination)
        destination = self.path / dataset_datadir / destination

        if destination.exists() and not destination.is_dir():
            raise errors.ParameterError(f'Destination is not a directory: "{destination}"')

        self.check_external_storage()

        files = []
        if all_at_once:  # Importing a dataset
            files = self._add_from_urls(
                dataset=dataset,
                urls=urls,
                destination_names=destination_names,
                destination=destination,
                extract=extract,
                progress=progress,
            )
        else:
            for url in urls:
                is_remote, is_git = _check_url(url)

                if is_git and is_remote:  # Remote git repo
                    sources = sources or ()
                    new_files = self._add_from_git(
                        dataset=dataset, url=url, sources=sources, destination=destination, ref=ref
                    )
                else:
                    if sources:
                        raise errors.UsageError('Cannot use "--source" with URLs or local files.')

                    if not is_remote:  # Local path, might be git
                        if is_git:
                            warning_messages.append(
                                "Adding data from local Git repository: "
                                + "Use remote's Git URL instead to enable "
                                + "lineage information and updates."
                            )
                        u = parse.urlparse(url)
                        new_files = self._add_from_local(
                            dataset=dataset, path=u.path, external=external, destination=destination
                        )
                    else:  # Remote URL
                        new_files = self._add_from_url(
                            dataset=dataset, url=url, destination=destination, extract=extract, progress=progress
                        )

                files.extend(new_files)

        # Remove all files that are under a .git directory
        paths_to_avoid = [f["path"] for f in files if ".git" in str(f["path"]).split(os.path.sep)]
        if paths_to_avoid:
            files = [f for f in files if f["path"] not in paths_to_avoid]
            warning_messages.append(
                "Ignored adding paths under a .git directory:\n  " + "\n  ".join(str(p) for p in paths_to_avoid)
            )

        files_to_commit = {str(self.path / f["path"]) for f in files}

        if not force:
            ignored_files = self.find_ignored_paths(*files_to_commit)
            if ignored_files:
                ignored_files = set(ignored_files)
                files_to_commit = files_to_commit.difference(ignored_files)
                ignored_sources = []
                for file_ in files:
                    if str(self.path / file_["path"]) in ignored_files:
                        operation = file_.get("operation")
                        if operation:
                            src, _, _ = operation
                            ignored_sources.append(src)
                        else:
                            ignored_sources.append(file_["path"])

                files = [f for f in files if str(self.path / f["path"]) in files_to_commit]
                warning_messages.append(
                    "Theses paths are ignored by one of your .gitignore "
                    + 'files (use "--force" flag if you really want to add '
                    + "them):\n  "
                    + "\n  ".join([str(p) for p in ignored_sources])
                )

        # all files at this point can be force-added

        if not overwrite:
            existing_files = dataset.find_files(files_to_commit)
            if existing_files:
                files_to_commit = files_to_commit.difference(existing_files)
                files = [f for f in files if str(self.path / f["path"]) in files_to_commit]
                warning_messages.append(
                    "These existing files were not overwritten "
                    + '(use "--overwrite" flag to overwrite them):\n  '
                    + "\n  ".join([str(p) for p in existing_files])
                )

        for data in files:
            operation = data.pop("operation", None)
            if not operation:
                continue

            src, dst, action = operation

            # Remove existing file if any
            self.remove_file(dst)
            dst.parent.mkdir(parents=True, exist_ok=True)

            if action == "copy":
                shutil.copy(src, dst)
            elif action == "move":
                shutil.move(src, dst, copy_function=shutil.copy)
            elif action == "symlink":
                self._create_external_file(src, dst)
                data["external"] = True
            else:
                raise errors.OperationError(f"Invalid action {action}")

        # Track non-symlinks in LFS
        if self.check_external_storage():
            lfs_paths = self.track_paths_in_storage(*files_to_commit)
            show_message = self.get_value("renku", "show_lfs_message")
            if lfs_paths and (show_message is None or show_message == "True"):
                messages.append(
                    (
                        "Adding these files to Git LFS:\n"
                        + "\t{}".format("\n\t".join(lfs_paths))
                        + "\nTo disable this message in the future, run:"
                        + "\n\trenku config show_lfs_message False"
                    )
                )

        # Force-add to include possible ignored files
        self.repo.git.add(*files_to_commit, force=True)
        self.repo.git.add(self.renku_pointers_path, force=True)

        staged_files = self.repo.index.diff("HEAD")
        if staged_files:
            msg = "renku dataset: committing {} newly added files".format(len(files_to_commit))
            skip_hooks = not self.external_storage_requested
            self.repo.index.commit(msg, skip_hooks=skip_hooks)
        else:
            warning_messages.append("No file was added to project")

        # Generate the DatasetFiles
        dataset_files = []
        for data in files:
            data.setdefault("url", generate_dataset_file_url(client=self, filepath=str(data["path"])))
            dataset_file = DatasetFile.from_revision(self, **data)

            # Set dataset file path relative to root for submodules.
            if dataset_file.client != self:
                dataset_file.path = str(data["path"])
            dataset_files.append(dataset_file)

        dataset.update_files(dataset_files)
        return warning_messages, messages

    def _check_protected_path(self, path):
        """Checks if a path is a protected path."""
        try:
            path_in_repo = path.relative_to(self.path)
        except ValueError:
            return False

        for protected_path in self.RENKU_PROTECTED_PATHS:
            str_path = str(path_in_repo)
            if re.match("^{}$".format(protected_path), str_path):
                return True

        return False

    def _add_from_local(self, dataset, path, external, destination):
        """Add a file or directory from a local filesystem."""
        src = Path(os.path.abspath(path))

        if not src.exists():
            raise errors.ParameterError(f"Cannot find file/directory: {path}")

        dst = destination / src.name

        # if we have a directory, recurse
        if src.is_dir():
            if dst.exists() and not dst.is_dir():
                raise errors.ParameterError(f'Cannot copy directory to a file: "{dst}"')
            if src == (self.path / dataset.data_dir).resolve():
                raise errors.ParameterError(f"Cannot add dataset's data directory recursively: {path}")

            if self._check_protected_path(src):
                raise errors.ProtectedFiles([src])

            files = []
            for f in src.iterdir():
                files.extend(
                    self._add_from_local(dataset=dataset, path=os.path.abspath(f), external=external, destination=dst)
                )
            return files
        else:
            # Check if file is in the project and return it
            path_in_repo = None
            if self._is_external_file(src):
                path_in_repo = path
            else:
                try:
                    path_in_repo = src.relative_to(self.path)
                except ValueError:
                    pass
                else:
                    if self._check_protected_path(src):
                        raise errors.ProtectedFiles([src])

            if path_in_repo:
                return [{"path": path_in_repo, "source": path_in_repo, "parent": self}]

        action = "symlink" if external else "copy"
        return [
            {
                "path": dst.relative_to(self.path),
                "source": os.path.relpath(str(src), str(self.path)),
                "parent": self,
                "operation": (src, dst, action),
            }
        ]

    def _add_from_urls(self, dataset, urls, destination, destination_names, extract, progress):
        files = []
        max_workers = min(os.cpu_count() - 1, 4) or 1
        with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(
                    self._add_from_url,
                    dataset=dataset,
                    url=url,
                    destination=destination,
                    extract=extract,
                    filename=name,
                    progress=progress,
                )
                for url, name in zip(urls, destination_names)
            }

            for future in concurrent.futures.as_completed(futures):
                files.extend(future.result())

        return files

    def _add_from_url(self, dataset, url, destination, extract, filename=None, progress=None):
        """Process adding from url and return the location on disk."""
        url = self._provider_check(url)

        try:
            start = time.time() * 1e3
            tmp_root, paths = self._download(url=url, filename=filename, extract=extract, progress_class=progress)

            exec_time = (time.time() * 1e3 - start) // 1e3
            # If execution time was less or equal to zero seconds,
            # block the thread a bit to avoid being rate limited.
            if exec_time == 0:
                time.sleep(min(os.cpu_count() - 1, 4) or 1)

        except (requests.exceptions.HTTPError, error.HTTPError) as e:  # pragma nocover
            raise errors.OperationError("Cannot download from {}".format(url)) from e

        paths = [(src, destination / src.relative_to(tmp_root)) for src in paths if not src.is_dir()]
        return [
            {
                "operation": (src, dst, "move"),
                "path": dst.relative_to(self.path),
                "source": remove_credentials(url),
                "parent": self,
            }
            for src, dst in paths
        ]

    def _add_from_git(self, dataset, url, sources, destination, ref):
        """Process adding resources from another git repository."""
        from renku import LocalClient

        u = parse.urlparse(url)

        sources = self._resolve_paths(u.path, sources)

        # Get all files from repo that match sources
        repo, repo_path = self.prepare_git_repo(url, ref)
        files = set()
        used_sources = set()
        for file in repo.head.commit.tree.traverse():
            path = file.path
            result = self._get_src_and_dst(path, repo_path, sources, destination, used_sources)

            if result:
                files.add(result)

        unused_sources = set(sources.keys()) - used_sources
        if unused_sources:
            unused_sources = {str(s) for s in unused_sources}
            raise errors.ParameterError("No such file or directory", param_hint=unused_sources)

        # Create metadata and move files to dataset
        results = []
        remote_client = LocalClient(repo_path)

        # Pull files from LFS
        paths = set()
        for path, src, _ in files:
            if src.is_dir():
                continue
            if src.is_symlink():
                try:
                    path = str(src.resolve().relative_to(repo_path))
                except ValueError:  # External file
                    pass
            paths.add(path)
        self._fetch_lfs_files(repo_path, paths)

        # Fetch metadata from Renku if any
        paths = {f[0] for f in files}
        metadata = self._fetch_files_metadata(remote_client, paths)

        for path, src, dst in files:
            if not src.is_dir():
                # Use original metadata if it exists
                based_on = metadata.get(path)
                if based_on:
                    based_on.url = url
                    based_on.based_on = None
                    based_on.source = url
                else:
                    based_on = DatasetFile.from_revision(remote_client, path=src, url=url, source=url)

                path_in_dst_repo = dst.relative_to(self.path)

                if remote_client._is_external_file(src):
                    operation = (src.resolve(), dst, "symlink")
                else:
                    operation = (src, dst, "copy")

                results.append(
                    {
                        "path": path_in_dst_repo,
                        "source": remove_credentials(url),
                        "parent": self,
                        "based_on": based_on,
                        "operation": operation,
                    }
                )

        return results

    @staticmethod
    def _ensure_dropbox(url):
        """Ensure dropbox url is set for file download."""
        if not isinstance(url, ParseResult):
            url = parse.urlparse(url)

        query = url.query or ""
        if "dl=0" in url.query:
            query = query.replace("dl=0", "dl=1")
        else:
            query += "dl=1"

        url = url._replace(query=query)
        return url

    def _provider_check(self, url):
        """Check additional provider related operations."""
        url = parse.urlparse(url)

        if "dropbox.com" in url.netloc:
            url = self._ensure_dropbox(url)

        return parse.urlunparse(url)

    def _resolve_paths(self, root_path, paths):
        """Check if paths are within a root path and resolve them."""
        result = OrderedDict()  # Used as an ordered-set
        for path in paths:
            r = self._resolve_path(root_path, path)
            result[r] = None
        return result

    def _resolve_path(self, root_path, path):
        """Check if a path is within a root path and resolve it."""
        try:
            root_path = Path(root_path).resolve()
            path = os.path.abspath(root_path / path)
            return Path(path).relative_to(root_path)
        except ValueError:
            raise errors.ParameterError("File {} is not within path {}".format(path, root_path))

    def _get_src_and_dst(self, path, repo_path, sources, dst_root, used_sources):
        is_wildcard = False

        if not sources:
            source = Path(".")
        else:
            source = None
            for s in sources.keys():
                try:
                    Path(path).relative_to(s)
                except ValueError:
                    if glob.globmatch(path, str(s), flags=glob.GLOBSTAR):
                        is_wildcard = True
                        source = path
                        used_sources.add(s)
                        break
                else:
                    source = s
                    used_sources.add(source)
                    break

            if not source:
                return

        src = repo_path / path
        source_name = Path(source).name
        relative_path = Path(path).relative_to(source)

        if src.is_dir() and is_wildcard:
            sources[source] = None
            used_sources.add(source)

        dst = dst_root / source_name / relative_path

        return (path, src, dst)

    def _fetch_lfs_files(self, repo_path, paths):
        """Fetch and checkout paths that are tracked by Git LFS."""
        repo_path = str(repo_path)
        try:
            output = run(("git", "lfs", "ls-files", "--name-only"), stdout=PIPE, cwd=repo_path, universal_newlines=True)
        except SubprocessError:
            return

        lfs_files = set(output.stdout.split("\n"))
        files = lfs_files & paths
        if not files:
            return

        try:
            for path in files:
                run(["git", "lfs", "pull", "--include", path], cwd=repo_path)
        except KeyboardInterrupt:
            raise
        except SubprocessError:
            pass

    @staticmethod
    def _fetch_files_metadata(client, paths):
        """Return metadata for files from paths."""
        files = {}

        if is_project_unsupported(client):
            return files

        migrate(client)

        for _, dataset in client.datasets.items():
            for file_ in dataset.files:
                path = file_.path
                if path in paths:
                    files[path] = file_
        return files

    def dataset_commits(self, dataset, max_results=None):
        """Gets the newest commit for a dataset or its files.

        Commits are returned sorted from newest to oldest.
        """
        paths = [(self.path / dataset.path / self.METADATA).resolve()]

        paths.extend(self.path / f.path for f in dataset.files)

        commits = self.repo.iter_commits(paths=paths, max_count=max_results)

        return commits

    def add_dataset_tag(self, dataset, tag, description="", force=False):
        """Adds a new tag to a dataset.

        Validates if the tag already exists and that the tag follows
        the same rules as docker tags.
        See https://docs.docker.com/engine/reference/commandline/tag/
        for a documentation of docker tag syntax.

        :raises: errors.ParameterError
        """
        if len(tag) > 128:
            raise errors.ParameterError("Tags can be at most 128 characters long.")

        if not re.match("^(?![.-])[a-zA-Z0-9_.-]{1,128}$", tag):
            raise errors.ParameterError(
                (
                    "Tag {} is invalid. \n"
                    "Only characters a-z, A-Z, 0-9, ., - and _ "
                    "are allowed. \nTag can't start with a . or -"
                ).format(tag)
            )

        if any(t for t in dataset.tags if t.name == tag):
            if force:
                # remove duplicate tag
                dataset.tags = [t for t in dataset.tags if t.name != tag]
            else:
                raise errors.ParameterError("Tag {} already exists".format(tag))

        latest_commit = list(self.dataset_commits(dataset, max_results=1))[0]

        tag = DatasetTag(name=tag, description=description, commit=latest_commit.hexsha, dataset=dataset.title)

        dataset.tags.append(tag)

        return dataset

    def remove_dataset_tags(self, dataset, tags):
        """Removes tags from a dataset."""
        tag_names = {t.name for t in dataset.tags}
        not_found = set(tags).difference(tag_names)

        if len(not_found) > 0:
            raise errors.ParameterError("Tags {} not found".format(", ".join(not_found)))
        dataset.tags = [t for t in dataset.tags if t.name not in tags]

        return dataset

    def update_dataset_files(self, files, ref, delete=False):
        """Update files and dataset metadata according to their remotes.

        :param files: List of files to be updated
        :param delete: Indicates whether to delete files or not

        :return: List of files that should be deleted
        """
        from renku import LocalClient

        visited_repos = {}
        updated_files = []
        deleted_files = []

        for file_ in files:
            if not file_.based_on:
                continue

            file_.based_on = DatasetFile.from_jsonld(file_.based_on)
            based_on = file_.based_on
            url = based_on.source
            if url in visited_repos:
                repo, repo_path, remote_client = visited_repos[url]
            else:
                repo, repo_path = self.prepare_git_repo(url, ref)
                remote_client = LocalClient(repo_path)
                visited_repos[url] = repo, repo_path, remote_client

            remote_file = self._fetch_file_metadata(remote_client, based_on.path)

            if not remote_file:
                try:
                    remote_file = DatasetFile.from_revision(
                        remote_client, path=based_on.path, source=None, added=based_on.added
                    )
                except KeyError:
                    raise errors.ParameterError("Cannot find file {} in the repo {}".format(based_on.source, url))

            commit_sha = self._get_commit_sha_from_label(based_on)
            remote_commit_sha = self._get_commit_sha_from_label(remote_file)
            if commit_sha != remote_commit_sha:
                src = Path(repo.working_dir) / based_on.path
                dst = self.renku_path.parent / file_.path

                if src.exists():
                    # Fetch file is it is tracked by Git LFS
                    self._fetch_lfs_files(repo_path, {based_on.path})
                    if remote_client._is_external_file(src):
                        self.remove_file(dst)
                        self._create_external_file(src.resolve(), dst)
                    else:
                        shutil.copy(src, dst)
                    file_.based_on.commit = remote_file.commit
                    file_.based_on._label = remote_file._label
                    updated_files.append(file_)
                else:
                    # File was removed or renamed
                    if delete:
                        self.remove_file(dst)
                    deleted_files.append(file_)

        if not updated_files and (not delete or not deleted_files):
            # Nothing to commit or update
            return deleted_files

        # Commit changes in files

        file_paths = {str(self.path / f.path) for f in updated_files + deleted_files}
        # Force-add to include possible ignored files that are in datasets
        self.repo.git.add(*(file_paths), force=True)
        skip_hooks = not self.external_storage_requested
        self.repo.index.commit(
            "renku dataset: updated {} files and deleted {} files".format(len(updated_files), len(deleted_files)),
            skip_hooks=skip_hooks,
        )

        # Update datasets' metadata

        modified_datasets = {}

        for file_ in updated_files:
            new_file = DatasetFile.from_revision(self, path=file_.path, based_on=file_.based_on, url=file_.url)
            file_.dataset.update_files([new_file])
            modified_datasets[file_.dataset.name] = file_.dataset

        if delete:
            for file_ in deleted_files:
                file_.dataset.unlink_file(file_.path)
                modified_datasets[file_.dataset.name] = file_.dataset

        for dataset in modified_datasets.values():
            dataset.to_yaml()

        return deleted_files

    def _create_external_file(self, src, dst):
        """Create a new external file."""
        try:
            pointer_file = self._create_pointer_file(target=src)
            relative = os.path.relpath(pointer_file, dst.parent)
            os.symlink(relative, dst)
        except OSError as e:
            raise errors.OperationError("Could not create symbolic link") from e

    def _create_pointer_file(self, target, checksum=None):
        """Create a new pointer file."""
        target = Path(target).resolve()

        if checksum is None:
            checksum = self._calculate_checksum(target)

        while True:
            filename = "{}-{}".format(uuid.uuid4(), checksum)
            path = self.renku_pointers_path / filename
            if not path.exists():
                break

        try:
            os.symlink(target, path)
        except FileNotFoundError:
            raise errors.ParameterError("Cannot find external file {}".format(target))

        return path

    def _calculate_checksum(self, filepath):
        try:
            return self.repo.git.hash_object(str(filepath))
        except GitCommandError:
            return None

    def update_external_files(self, records):
        """Update files linked to external storage."""
        updated_files_paths = []
        updated_datasets = {}

        for file_ in records:
            if file_.external:
                path = self.path / file_.path
                link = path.parent / os.readlink(path)
                pointer_file = self.path / link
                pointer_file = self._update_pointer_file(pointer_file)
                if pointer_file is not None:
                    relative = os.path.relpath(pointer_file, path.parent)
                    os.remove(path)
                    os.symlink(relative, path)
                    updated_files_paths.append(str(path))
                    updated_datasets[file_.dataset.name] = file_.dataset

        if not updated_files_paths:
            return

        self.repo.git.add(*updated_files_paths, force=True)
        self.repo.git.add(self.renku_pointers_path, force=True)
        commit = self.repo.index.commit("renku dataset: updated {} external files".format(len(updated_files_paths)))

        for dataset in updated_datasets.values():
            for file_ in dataset.files:
                if str(self.path / file_.path) in updated_files_paths:
                    file_.commit = commit
                    file_._label = file_.default_label()
            dataset.to_yaml()

    def _update_pointer_file(self, pointer_file_path):
        """Update a pointer file."""
        try:
            target = pointer_file_path.resolve(strict=True)
        except FileNotFoundError:
            target = pointer_file_path.resolve()
            raise errors.ParameterError("External file not found: {}".format(target))

        checksum = self._calculate_checksum(target)
        current_checksum = pointer_file_path.name.split("-")[-1]

        if checksum == current_checksum:
            return

        os.remove(pointer_file_path)
        return self._create_pointer_file(target, checksum=checksum)

    def remove_file(self, filepath):
        """Remove a file/symlink and its pointer file (for external files)."""
        path = Path(filepath)
        try:
            link = path.parent / os.readlink(path)
        except FileNotFoundError:
            return
        except OSError:  # not a symlink but a normal file
            os.remove(path)
            return

        os.remove(path)

        try:
            os.remove(link)
        except FileNotFoundError:
            pass

    def _is_external_file(self, path):
        """Checks if a path within repo is an external file."""
        if not Path(path).is_symlink() or not self._is_path_within_repo(path):
            return False
        pointer = os.readlink(path)
        return f"{self.renku_home}/{self.POINTERS}" in pointer

    def has_external_files(self):
        """Return True if project has external files."""
        for dataset in self.datasets.values():
            for file_ in dataset.files:
                if file_.external:
                    return True

    def _is_path_within_repo(self, path):
        if not os.path.isabs(path):
            path = os.path.abspath(path)
        path = Path(path)
        try:
            path.relative_to(self.path)
        except ValueError:
            return False
        else:
            return True

    def prepare_git_repo(self, url, ref=None):
        """Clone and cache a Git repo."""
        if not url:
            raise errors.GitError("Invalid URL.")

        RENKU_BRANCH = "renku-default-branch"

        def checkout(repo, ref):
            try:
                repo.git.checkout(ref)
            except GitCommandError:
                raise errors.ParameterError('Cannot find reference "{}" in Git repository: {}'.format(ref, url))

        ref = ref or RENKU_BRANCH
        u = GitURL.parse(url)
        path = u.pathname
        if u.hostname == "localhost":
            path = str(Path(path).resolve())
            url = path
        repo_name = os.path.splitext(os.path.basename(path))[0]
        path = os.path.dirname(path).lstrip("/")
        repo_path = self.renku_path / self.CACHE / u.hostname / path / repo_name

        if repo_path.exists():
            repo = Repo(str(repo_path))
            if repo.remotes.origin.url == url:
                try:
                    repo.git.fetch(all=True)
                    repo.git.checkout(ref)
                    try:
                        repo.git.pull()
                    except GitError:
                        # When ref is not a branch, an error is thrown
                        pass
                except GitError:
                    # ignore the error and try re-cloning
                    pass
                else:
                    return repo, repo_path

            try:
                shutil.rmtree(str(repo_path))
            except PermissionError:
                raise errors.InvalidFileOperation("Cannot delete files in {}: Permission denied".format(repo_path))

        repo = clone(url, path=str(repo_path), install_githooks=False)

        # Because the name of the default branch is not always 'master', we
        # create an alias of the default branch when cloning the repo. It
        # is used to refer to the default branch later.
        renku_ref = "refs/heads/" + RENKU_BRANCH
        try:
            repo.git.execute(["git", "symbolic-ref", renku_ref, repo.head.reference.path])
            checkout(repo, ref)
        except GitCommandError as e:
            raise errors.GitError("Cannot clone remote Git repo: {}".format(url)) from e
        else:
            return repo, repo_path

    @staticmethod
    def _fetch_file_metadata(client, path):
        """Return metadata for a single file."""
        for _, dataset in client.datasets.items():
            for file_ in dataset.files:
                if file_.path == path:
                    return file_

    @staticmethod
    def _get_commit_sha_from_label(dataset_file):
        label = dataset_file._label
        if "@" in label:
            return label.split("@")[1]
        return label

    def _download(self, url, filename, extract, progress_class=None, chunk_size=16384):
        def extract_dataset(filepath):
            """Extract downloaded file."""
            try:
                tmp = tempfile.mkdtemp()
                patoolib.extract_archive(str(filepath), outdir=tmp, verbosity=-1)
            except patoolib.util.PatoolError:
                return filepath.parent, [filepath]
            else:
                filepath.unlink()
                return Path(tmp), [p for p in Path(tmp).rglob("*")]

        tmp_root = self.renku_path / self.CACHE
        tmp_root.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.mkdtemp(dir=tmp_root)

        with requests.get(url, stream=True) as request:
            request.raise_for_status()

            if not filename:
                u = parse.urlparse(url)
                filename = Path(u.path).name
                if not filename:
                    raise errors.ParameterError(f"URL Cannot find a file to download from {url}")

            download_to = Path(tmp) / filename
            with open(str(download_to), "wb") as file_:
                total_size = int(request.headers.get("content-length", 0))
                progress_class = progress_class or DownloadProgressCallback
                progress = progress_class(description=filename, total_size=total_size)

                try:
                    for chunk in request.iter_content(chunk_size=chunk_size):
                        if chunk:  # ignore keep-alive chunks
                            file_.write(chunk)
                            progress.update(size=len(chunk))
                finally:
                    progress.finalize()
        if extract:
            return extract_dataset(download_to)

        return download_to.parent, [download_to]


class DownloadProgressCallback:
    """Interface to report various stages of a download."""

    def __init__(self, description, total_size):
        """Default initializer."""

    def update(self, size):
        """Update the status."""

    def finalize(self):
        """Called once when the download is finished."""


def _check_url(url):
    """Check if a url is local/remote and if it contains a git repository."""
    u = parse.urlparse(url)

    if u.scheme not in Dataset.SUPPORTED_SCHEMES:
        raise errors.UrlSchemeNotSupported('Scheme "{}" not supported'.format(u.scheme))

    is_remote = u.scheme not in ("", "file") or url.startswith("git@")
    is_git = False

    if is_remote:
        is_git = u.path.endswith(".git")
    else:
        try:
            Repo(u.path, search_parent_directories=True)
        except GitError:
            pass
        else:
            is_git = True

    return is_remote, is_git


DATASET_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS),
    Path(RENKU_HOME) / Path(DatasetsApiMixin.POINTERS),
    Path(RENKU_HOME) / Path(LinkReference.REFS),
    ".gitattributes",
]
