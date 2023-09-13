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
"""Git dataset provider."""

import glob
import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from renku.core import errors
from renku.core.dataset.pointer_file import create_external_file
from renku.core.dataset.providers.api import AddProviderInterface, ProviderApi, ProviderPriority
from renku.core.lfs import pull_paths_from_storage
from renku.core.util import communication
from renku.core.util.git import clone_repository, get_cache_directory_for_repository
from renku.core.util.metadata import is_linked_file
from renku.core.util.os import delete_dataset_file, get_files, is_subpath
from renku.core.util.urls import check_url, remove_credentials
from renku.domain_model.dataset import RemoteEntity
from renku.domain_model.project_context import project_context
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata, DatasetUpdateMetadata, ProviderParameter


class GitProvider(ProviderApi, AddProviderInterface):
    """Git provider."""

    priority = ProviderPriority.NORMAL
    name = "Git"
    is_remote = True

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        is_remote, is_git = check_url(uri)
        return is_remote and is_git

    @staticmethod
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter(
                "sources",
                flags=["s", "source"],
                default=None,
                help="Path(s) within remote git repo to be added",
                multiple=True,
            ),
            ProviderParameter(
                "revision",
                flags=["r", "ref"],
                default=None,
                help="Add files from a specific commit/tag/branch.",
                type=str,
            ),
        ]

    def get_metadata(
        self,
        uri: str,
        destination: Path,
        *,
        sources: Optional[List[Union[Path, str]]] = None,
        revision: Optional[str] = None,
        **kwargs,
    ) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata

        destination_exists = destination.exists()
        destination_is_dir = destination.is_dir()

        remote_repository = clone_repository(
            url=uri,
            path=get_cache_directory_for_repository(url=uri),
            checkout_revision=revision,
            depth=None,
            clean=True,
        )

        def check_sources_are_within_remote_repo():
            if not sources:
                return
            for source in sources:
                if not is_subpath(path=remote_repository.path / source, base=remote_repository.path):
                    raise errors.ParameterError(f"Path '{source}' is not within the repository")

        def get_source_paths() -> Set[Path]:
            """Return all paths from the repo that match a source pattern."""
            if not sources:
                return set(remote_repository.path.glob("*"))  # type: ignore

            paths = set()
            for source in sources:
                # NOTE: Normalized source to resolve .. references (if any). This preserves wildcards.
                normalized_source = os.path.normpath(source)
                absolute_source = os.path.join(remote_repository.path, normalized_source)  # type: ignore
                # NOTE: Path.glob("root/**") does not return correct results (e.g. it include ``root`` in the result)
                subpaths = {Path(p) for p in glob.glob(absolute_source)}
                if len(subpaths) == 0:
                    raise errors.ParameterError("No such file or directory", param_hint=str(source))
                paths |= subpaths

            return paths

        def should_copy(source_paths: List[Path]) -> bool:
            n_paths = len(source_paths)
            has_multiple_sources = n_paths > 1
            source_is_dir = has_multiple_sources or (n_paths == 1 and source_paths[0].is_dir())

            if source_is_dir and destination_exists and not destination_is_dir:
                raise errors.ParameterError(f"Destination is not a directory: '{destination}'")

            return has_multiple_sources or (destination_exists and destination_is_dir)

        def get_file_metadata(src: Path, dst: Path) -> Optional["DatasetAddMetadata"]:
            path_in_src_repo = src.relative_to(remote_repository.path)  # type: ignore
            path_in_dst_repo = dst.relative_to(project_context.path)

            already_copied = path_in_dst_repo in new_files  # A path with the same destination is already copied
            new_files[path_in_dst_repo].append(path_in_src_repo)
            if already_copied:
                return None

            checksum = remote_repository.get_object_hash(revision="HEAD", path=path_in_src_repo)  # type: ignore
            if not checksum:
                raise errors.FileNotFound(f"Cannot find '{file}' in the remote project")

            return DatasetAddMetadata(
                entity_path=path_in_dst_repo,
                url=remove_credentials(uri),
                based_on=RemoteEntity(checksum=checksum, path=path_in_src_repo, url=uri),
                action=DatasetAddAction.MOVE,
                source=src,
                destination=dst,
            )

        check_sources_are_within_remote_repo()

        results = []
        new_files: Dict[Path, List[Path]] = defaultdict(list)

        paths = get_source_paths()
        with project_context.with_path(remote_repository.path):
            pull_paths_from_storage(project_context.repository, *paths)
        is_copy = should_copy(list(paths))

        for path in paths:
            dst_root = destination / path.name if is_copy else destination

            for file in get_files(path):
                src = file
                relative_path = file.relative_to(path)
                dst = dst_root / relative_path

                metadata = get_file_metadata(src, dst)
                if metadata:
                    results.append(metadata)

        duplicates = [v for v in new_files.values() if len(v) > 1]
        if duplicates:
            files = {str(p) for paths in duplicates for p in paths}
            files_str = "/n/t".join(sorted(files))
            communication.warn(f"The following files overwrite each other in the destination project:/n/t{files_str}")

        return results

    def update_files(
        self,
        files: List[DynamicProxy],
        dry_run: bool,
        delete: bool,
        context: Dict[str, Any],
        ref: Optional[str] = None,
        **kwargs,
    ) -> List["DatasetUpdateMetadata"]:
        """Update dataset files from the remote provider."""
        from renku.core.dataset.providers.models import DatasetUpdateAction, DatasetUpdateMetadata

        if "visited_repos" not in context:
            context["visited_repos"] = {}

        progress_text = "Checking git files for updates"

        results: List[DatasetUpdateMetadata] = []

        try:
            communication.start_progress(progress_text, len(files))
            for file in files:
                communication.update_progress(progress_text, 1)
                if not file.based_on:
                    continue

                based_on = file.based_on
                url = based_on.url
                if url in context["visited_repos"]:
                    remote_repository = context["visited_repos"][url]
                else:
                    communication.echo(msg="Cloning remote repository...")
                    path = get_cache_directory_for_repository(url=url)
                    remote_repository = clone_repository(url=url, path=path, checkout_revision=ref)
                    context["visited_repos"][url] = remote_repository

                checksum = remote_repository.get_object_hash(path=based_on.path, revision="HEAD")
                found = checksum is not None
                changed = found and based_on.checksum != checksum

                src = remote_repository.path / based_on.path
                dst = project_context.metadata_path.parent / file.entity.path

                if not found:
                    if not dry_run and delete:
                        delete_dataset_file(dst, follow_symlinks=True)
                        project_context.repository.add(dst, force=True)
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))
                elif changed:
                    if not dry_run:
                        # Fetch file if it is tracked by Git LFS
                        pull_paths_from_storage(remote_repository, remote_repository.path / based_on.path)
                        if is_linked_file(path=src, project_path=remote_repository.path):
                            delete_dataset_file(dst, follow_symlinks=True)
                            create_external_file(target=src.resolve(), path=dst)
                        else:
                            shutil.copy(src, dst)
                        file.based_on = RemoteEntity(
                            checksum=checksum, path=based_on.path, url=based_on.url  # type: ignore
                        )
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.UPDATE))
        finally:
            communication.finalize_progress(progress_text)

        return results
