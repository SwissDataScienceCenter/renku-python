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
"""Dataset business logic."""

import os
import shutil
import urllib
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

import filetype
import patoolib
from pydantic import ConfigDict, validate_call

from renku.command.command_builder.command import inject
from renku.command.view_model.dataset import DatasetFileViewModel, DatasetViewModel
from renku.core import errors
from renku.core.config import get_value, remove_value, set_value
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import delete_external_file, is_linked_file_updated, update_linked_file
from renku.core.dataset.providers.api import AddProviderInterface, ProviderApi
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.git import GitProvider
from renku.core.dataset.providers.models import DatasetUpdateAction, ProviderDataset
from renku.core.dataset.tag import get_dataset_by_tag, prompt_access_token, prompt_tag_selection
from renku.core.image import ImageObjectRequest
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.lfs import check_external_storage, track_paths_in_storage
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.git import get_git_user
from renku.core.util.metadata import prompt_for_credentials, read_credentials, store_credentials
from renku.core.util.os import (
    create_symlink,
    delete_dataset_file,
    delete_path,
    get_absolute_path,
    get_file_size,
    get_files,
    get_relative_path,
    get_safe_relative_path,
    hash_file,
    is_path_empty,
    is_subpath,
    unmount_path,
)
from renku.core.util.tabulate import tabulate
from renku.core.util.urls import get_slug
from renku.core.util.util import parallel_execute
from renku.domain_model.constant import NO_VALUE, NON_EXISTING_ENTITY_CHECKSUM, NoValueType
from renku.domain_model.dataset import Dataset, DatasetDetailsJson, DatasetFile, RemoteEntity, is_dataset_slug_valid
from renku.domain_model.entity import Entity
from renku.domain_model.enums import ConfigFilter
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.interface.storage import IStorage


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def search_datasets(slug: str) -> List[str]:
    """Get all the datasets whose slug starts with the given string.

    Args:
        slug(str): Beginning of dataset slug to search for.

    Returns:
        List[str]: List of found dataset slugs.
    """
    datasets_provenance = DatasetsProvenance()
    return list(filter(lambda x: x.startswith(slug), map(lambda x: x.slug, datasets_provenance.datasets)))


def list_datasets():
    """List all datasets."""
    datasets_provenance = DatasetsProvenance()
    datasets = []

    for dataset in datasets_provenance.datasets:
        tags = datasets_provenance.get_all_tags(dataset)
        dataset = cast(Dataset, DynamicProxy(dataset))
        dataset.tags = tags
        dataset.tags_csv = ",".join(tag.name for tag in tags)
        dataset.datadir_path = str(dataset.get_datadir())
        datasets.append(dataset)

    return list(datasets)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def create_dataset(
    slug: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    creators: Optional[List[Person]] = None,
    keywords: Optional[List[str]] = None,
    images: Optional[List[ImageObjectRequest]] = None,
    update_provenance: bool = True,
    custom_metadata: Optional[Dict[str, Any]] = None,
    storage: Optional[str] = None,
    datadir: Optional[Path] = None,
):
    """Create a dataset.

    Args:
        slug(str): Slug of the dataset
        name(Optional[str], optional): Dataset name (Default value = None).
        description(Optional[str], optional): Dataset description (Default value = None).
        creators(Optional[List[Person]], optional): Dataset creators (Default value = None).
        keywords(Optional[List[str]], optional): Dataset keywords (Default value = None).
        images(Optional[List[ImageObjectRequest]], optional): Dataset images (Default value = None).
        update_provenance(bool, optional): Whether to add this dataset to dataset provenance
            (Default value = True).
        custom_metadata(Optional[Dict[str, Any]], optional): Custom JSON-LD metadata (Default value = None).
        storage(Optional[str], optional): Backend storage's URI (Default value = None).
        datadir(Optional[Path]): Dataset's data directory (Default value = None).

    Returns:
        Dataset: The created dataset.
    """
    if not creators:
        creators = []
        user = get_git_user(repository=project_context.repository)

        if user:
            creators.append(user)

    if not is_dataset_slug_valid(slug):
        valid_slug = get_slug(slug, lowercase=False)
        raise errors.ParameterError(f"Dataset slug '{slug}' is not valid (Hint: '{valid_slug}' is valid).")

    datasets_provenance = DatasetsProvenance()

    if datasets_provenance.get_by_slug(slug=slug):
        raise errors.DatasetExistsError(slug)

    if not name:
        name = slug

    keywords = keywords or []

    annotations = (
        [Annotation(id=Annotation.generate_id(), source="renku", body=custom_metadata)] if custom_metadata else None
    )

    if datadir:
        try:
            datadir = get_safe_relative_path(datadir, project_context.path)
        except ValueError as e:
            raise errors.ParameterError("Datadir must be inside repository.") from e

    dataset = Dataset(
        identifier=None,
        slug=slug,
        name=name,
        description=description,
        creators=creators,
        keywords=keywords,
        project_id=project_context.project.id,
        annotations=annotations,
        storage=storage,
        datadir=datadir,
    )

    if images:
        set_dataset_images(dataset=dataset, images=images)

    if storage:
        provider = ProviderFactory.get_create_provider(uri=storage)
        provider.on_create(dataset=dataset)
    else:
        add_datadir_files_to_dataset(dataset)

    if update_provenance:
        datasets_provenance.add_or_update(dataset)

    return dataset


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def edit_dataset(
    slug: str,
    name: Optional[Union[str, NoValueType]],
    description: Optional[Union[str, NoValueType]],
    creators: Optional[Union[List[Person], NoValueType]],
    keywords: Optional[Union[List[str], NoValueType]] = NO_VALUE,
    images: Optional[Union[List[ImageObjectRequest], NoValueType]] = NO_VALUE,
    custom_metadata: Optional[Union[Dict, List[Dict], NoValueType]] = NO_VALUE,
    custom_metadata_source: Optional[Union[str, NoValueType]] = NO_VALUE,
):
    """Edit dataset metadata.

    Args:
        slug(str): Slug of the dataset to edit
        name(Optional[Union[str, NoValueType]]): New name for the dataset.
        description(Optional[Union[str, NoValueType]]): New description for the dataset.
        creators(Optional[Union[List[Person], NoValueType]]): New creators for the dataset.
        keywords(Optional[Union[List[str], NoValueType]]): New keywords for dataset (Default value = ``NO_VALUE``).
        images(Optional[Union[List[ImageObjectRequest], NoValueType]]): New images for dataset
            (Default value = ``NO_VALUE``).
        custom_metadata(Optional[Union[Dict, List[Dict], NoValueType]]): Custom JSON-LD metadata
            (Default value = ``NO_VALUE``).
        custom_metadata_source(Optional[Union[str, NoValueType]]): The custom metadata source
            (Default value = ``NO_VALUE``).

    Returns:
        bool: True if updates were performed.
    """
    if isinstance(name, str):
        name = name.strip()

    if name is None:
        name = ""

    possible_updates = {
        "creators": creators,
        "description": description,
        "keywords": keywords,
        "name": name,
    }

    dataset_provenance = DatasetsProvenance()
    dataset = dataset_provenance.get_by_slug(slug=slug)

    if dataset is None:
        raise errors.ParameterError("Dataset does not exist.")

    updated: Dict[str, Any] = {k: v for k, v in possible_updates.items() if v != NO_VALUE}

    if updated:
        dataset.update_metadata(creators=creators, description=description, keywords=keywords, name=name)

    if images == NO_VALUE:
        images_updated = False
    else:
        images_updated = set_dataset_images(dataset=dataset, images=cast(Optional[List[ImageObjectRequest]], images))

    if images_updated:
        updated["images"] = (
            None if images is None else [{"content_url": i.content_url, "position": i.position} for i in dataset.images]
        )

    if custom_metadata is not NO_VALUE:
        update_dataset_custom_metadata(
            dataset,
            cast(Optional[Union[Dict, List[Dict]]], custom_metadata),
            cast(Optional[str], custom_metadata_source),
        )
        updated["custom_metadata"] = custom_metadata

    if not updated:
        return []

    datasets_provenance = DatasetsProvenance()
    datasets_provenance.add_or_update(dataset, creator=get_git_user(project_context.repository))

    return updated


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def list_dataset_files(
    datasets: Optional[List[str]] = None,
    tag: Optional[str] = None,
    creators: Optional[Union[str, List[str], Tuple[str]]] = None,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
):
    """List dataset files.

    Args:
        datasets(Optional[List[str]]): Datasets to list files for (Default value = None).
        tag(str): Tag to filter by (Default value = None).
        creators(Optional[Union[str, List[str], Tuple[str]]]): Creators to filter by (Default value = None).
        include(Optional[List[str]]): Include filters for file paths (Default value = None).
        exclude(Optional[List[str]]): Exclude filters for file paths (Default value = None).

    Returns:
        List[DynamicProxy]: Filtered dataset files.
    """
    from renku.command.format.dataset_files import get_lfs_tracking_and_file_sizes

    records = filter_dataset_files(
        slugs=datasets, tag=tag, creators=creators, include=include, exclude=exclude, immutable=True
    )
    for record in records:
        record.title = record.dataset.name
        record.dataset_slug = record.dataset.slug
        record.dataset_id = record.dataset.id
        record.creators_csv = record.dataset.creators_csv
        record.creators_full_csv = record.dataset.creators_full_csv
        record.full_path = project_context.path / record.entity.path
        record.path = record.entity.path
        record.name = Path(record.entity.path).name
        record.added = record.date_added

    get_lfs_tracking_and_file_sizes(records, has_tag=bool(tag))

    return records


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def file_unlink(
    slug: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    yes: bool = False,
    dataset_files: Optional[List[DatasetFile]] = None,
):
    """Remove matching files from a dataset.

    Args:
        slug(str): Dataset slug.
        include(Optional[List[str]]): Include filter for files (Default value = None).
        exclude(Optional[List[str]]): Exclude filter for files (Default value = None).
        yes(bool): Whether to skip user confirmation or not (Default value = False).
        dataset_files(Optional[List[DatasetFile]]): Files to remove; ignore include and exclude if passed (Default value
            = None).

    Returns:
        List[DynamicProxy]: List of files that were removed.
    """
    repository = project_context.repository

    if not include and not exclude and not dataset_files:
        raise errors.ParameterError("include or exclude filters not specified.")

    datasets_provenance = DatasetsProvenance()

    dataset = datasets_provenance.get_by_slug(slug=slug)

    if not dataset:
        raise errors.ParameterError("Dataset does not exist.")

    records = []
    if not dataset_files:
        records = filter_dataset_files(slugs=[slug], include=include, exclude=exclude)
        if not records:
            raise errors.ParameterError("No records found.")
        dataset_files = [cast(DatasetFile, r) for r in records]

    if not yes:
        prompt_text = (
            f'You are about to remove following from "{slug}" dataset.'
            + "\n"
            + "\n".join([str(record.entity.path) for record in dataset_files])
            + "\nDo you wish to continue?"
        )
        communication.confirm(prompt_text, abort=True, warning=True)

    for file in dataset_files:
        dataset.unlink_file(file.entity.path)
        path_file = Path(file.entity.path)

        if file.is_external or file.linked:
            try:
                delete_external_file(file)
            except errors.InvalidFileOperation as e:
                communication.warn(f"Cannot delete dataset file {path_file}: {e}.")
        elif dataset.is_within_datadir(path_file):  # NOTE: Remove dataset file only if it's inside dataset's datadir
            datadir = dataset.get_datadir()

            if not path_file.exists():
                communication.warn(f"Dataset file {path_file} doesn't exist, skipping the removal from {datadir}.")
                continue

            try:
                if path_file.is_dir():
                    shutil.rmtree(str(path_file.absolute()), ignore_errors=False, onerror=None)
                else:
                    path_file.unlink()
            except Exception as err:
                communication.warn(f"Dataset file {path_file} could not be removed from {datadir} because of {err}.")

        repository.add(path_file)

    datasets_provenance.add_or_update(dataset, creator=get_git_user(repository))

    return records


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def remove_dataset(slug: str):
    """Delete a dataset.

    Args:
        slug(str): Slug of dataset to delete.
    """
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_slug(slug=slug, strict=True)
    datasets_provenance.remove(dataset=dataset)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def export_dataset(slug: str, provider_name: str, tag: Optional[str], **kwargs):
    """Export data to 3rd party provider.

    Args:
        slug(str): Slug of dataset to export.
        provider_name(str): Provider to use for export.
        tag(str): Dataset tag from which to export.
    """
    datasets_provenance = DatasetsProvenance()

    provider_name = provider_name.lower()

    # TODO: all these callbacks are ugly, improve in #737
    config_key_secret = "access_token"  # nosec

    dataset: Optional[Dataset] = datasets_provenance.get_by_slug(slug, strict=True, immutable=True)

    provider = ProviderFactory.get_export_provider(provider_name=provider_name)

    selected_tag = None
    tags = datasets_provenance.get_all_tags(dataset)  # type: ignore

    if tag:
        selected_tag = next((t for t in tags if t.name == tag), None)

        if not selected_tag:
            raise errors.ParameterError(f"Tag '{tag}' not found for dataset '{slug}'")
    elif tags:
        selected_tag = prompt_tag_selection(tags)

    if selected_tag:
        dataset = datasets_provenance.get_by_id(selected_tag.dataset_id.value, immutable=True)

        if not dataset:
            raise errors.DatasetNotFound(message=f"Cannot find dataset with id: '{selected_tag.dataset_id.value}'")

    dataset = cast(Dataset, DynamicProxy(dataset))

    exporter = provider.get_exporter(dataset=dataset, tag=selected_tag, **kwargs)

    if exporter.requires_access_token():
        access_token = read_credentials(section=provider_name, key=config_key_secret)

        if access_token is None:
            access_token = prompt_access_token(exporter)

            if access_token is None or len(access_token) == 0:
                raise errors.InvalidAccessToken()

            store_credentials(section=provider_name, key=config_key_secret, value=access_token)

        exporter.set_access_token(access_token)

    try:
        destination = exporter.export()
    except errors.AuthenticationError:
        remove_value(provider_name, config_key_secret, global_only=True)
        raise

    communication.echo(f"Exported to: {destination}")


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def import_dataset(
    uri: str,
    slug: Optional[str] = "",
    extract: bool = False,
    yes: bool = False,
    datadir: Optional[Path] = None,
    previous_dataset: Optional[Dataset] = None,
    delete: bool = False,
    gitlab_token: Optional[str] = None,
    **kwargs,
):
    """Import data from a 3rd party provider or another renku project.

    Args:
        uri(str): DOI or URL of dataset to import.
        slug(str): Slug to give to the imported dataset (Default value = "").
        extract(bool): Whether to extract compressed dataset data (Default value = False).
        yes(bool): Whether to skip user confirmation (Default value = False).
        datadir(Optional[Path]): Dataset's data directory (Default value = None).
        previous_dataset(Optional[Dataset]): Previously imported dataset version (Default value = None).
        delete(bool): Whether to delete files that don't exist anymore (Default value = False).
        gitlab_token(Optional[str]): Gitlab OAuth2 token (Default value = None).
    """
    from renku.core.dataset.dataset_add import add_to_dataset

    def confirm_download(files):
        if yes:
            return

        headers = {"checksum": "checksum", "filename": "name", "filesize_str": "size", "filetype": "type"}
        communication.echo(tabulate(files, headers=headers, floatfmt=".2f"))
        communication.confirm("Do you wish to download this version?", abort=True, warning=True)

    def calculate_total_size(files) -> int:
        total_size = 0
        for file in files:
            if file.filesize is not None:
                total_size += file.filesize

        return total_size

    def remove_files(dataset):
        """Remove files that exist in ``previous_dataset`` but not in ``dataset``.

        Args:
            dataset(Dataset): Dataset to update.
        """
        if not delete or not previous_dataset:
            return

        current_paths = {str(f.entity.path) for f in dataset.files}
        previous_paths = {str(f.entity.path) for f in previous_dataset.files}
        deleted_paths = previous_paths - current_paths

        for path in deleted_paths:
            delete_dataset_file(project_context.path / path, follow_symlinks=True)

    provider = ProviderFactory.get_import_provider(uri)

    try:
        importer = provider.get_importer(gitlab_token=gitlab_token, **kwargs)
        provider_dataset: ProviderDataset = importer.fetch_provider_dataset()
    except KeyError as e:
        raise errors.ParameterError(f"Could not process '{uri}'.\nUnable to fetch metadata: {e}")
    except LookupError as e:
        raise errors.ParameterError(f"Could not process '{uri}'.\nReason: {e}")

    if not importer.provider_dataset_files:
        raise errors.ParameterError(f"Dataset '{uri}' has no files.")

    confirm_download(importer.provider_dataset_files)

    try:
        if not importer.is_latest_version():
            communication.warn(f"Newer version found at {importer.latest_uri}")
    except (KeyError, LookupError):
        pass

    if datadir and previous_dataset:
        raise errors.ParameterError("Can't specify datadir when updating a previously imported dataset.")
    elif datadir:
        try:
            datadir = get_safe_relative_path(datadir, project_context.path)
        except ValueError as e:
            raise errors.ParameterError("Datadir must be inside repository.") from e

    slug = slug or provider_dataset.slug

    new_dataset = add_to_dataset(
        dataset_slug=slug,
        urls=[],
        importer=importer,
        create=not previous_dataset,
        force=True,  # NOTE: Force-add to include any ignored files
        extract=extract,
        overwrite=True,
        total_size=calculate_total_size(importer.provider_dataset_files),
        clear_files_before=True,
        datadir=datadir,
        storage=provider_dataset.storage,
    )

    new_dataset.update_metadata_from(provider_dataset)
    # NOTE: Remove derived_from because this is an updated and imported dataset and won't be a derivative
    new_dataset.derived_from = None

    remove_files(new_dataset)

    importer.tag_dataset(slug)
    importer.copy_extra_metadata(new_dataset)

    project_context.database.commit()


@inject.autoparams()
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def update_datasets(
    slugs: List[str],
    creators: Optional[str],
    include: Optional[List[str]],
    exclude: Optional[List[str]],
    ref: Optional[str],
    delete: bool,
    no_local: bool,
    no_remote: bool,
    check_data_directory: bool,
    update_all: bool,
    dry_run: bool,
    plain: bool,
    dataset_gateway: IDatasetGateway,
) -> Tuple[List[DatasetViewModel], List[DatasetFileViewModel]]:
    """Update dataset files.

    Args:
        slugs(List[str]): Slugs of datasets to update.
        creators(Optional[str]): Creators to filter dataset files by.
        include(Optional[List[str]]): Include filter for paths to update.
        exclude(Optional[List[str]]): Exclude filter for paths to update.
        ref(Optional[str]): Git reference to use for update.
        delete(bool): Whether to delete files that don't exist on remote anymore.
        no_local(bool): Whether to exclude local files from the update.
        no_remote(bool): Whether to exclude remote files from the update.
        check_data_directory(bool): Whether to check the dataset's data directory for new files.
        update_all(bool): Whether to update all datasets.
        dry_run(bool): Whether to return a preview of what would be updated.
        plain(bool): Whether plain output should be produced.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
    """
    from renku.core.dataset.providers.renku import RenkuProvider

    if not update_all and not slugs and not include and not exclude and not dry_run:
        raise errors.ParameterError("No update criteria is specified")

    imported_dataset_updates: List[Dataset] = []

    all_datasets = dataset_gateway.get_all_active_datasets()
    imported_datasets = [d for d in all_datasets if d.same_as]

    if slugs and update_all:
        raise errors.ParameterError("Cannot pass dataset slugs when updating all datasets")
    elif (include or exclude) and update_all:
        raise errors.ParameterError("Cannot specify include and exclude filters when updating all datasets")
    elif (include or exclude) and slugs and any(d for d in imported_datasets if d.slug in slugs):
        raise errors.IncompatibleParametersError(first_param="--include/--exclude", second_param="imported datasets")

    slugs = slugs or [d.slug for d in all_datasets]

    # NOTE: update imported datasets
    if not include and not exclude and not no_remote:
        must_match_records = False

        for dataset in imported_datasets:
            if dataset.slug not in slugs:
                continue

            uri = dataset.same_as.value  # type: ignore
            try:
                provider = ProviderFactory.get_import_provider(uri)
            except errors.DatasetProviderNotFound:
                continue

            record = provider.get_importer()

            if isinstance(provider, RenkuProvider) and dataset.version is not None:
                tags = dataset_gateway.get_all_tags(dataset=dataset)
                tag = next((t for t in tags if t.name == dataset.version), None)
                # NOTE: Do not update Renku dataset that are imported from a specific version
                if tag is not None and tag.dataset_id.value == dataset.id:
                    communication.echo(
                        f"Skipped updating imported Renku dataset '{dataset.slug}' with tag '{tag.name}'"
                    )
                    slugs.remove(dataset.slug)
                    continue

            if record.is_latest_version() and record.is_version_equal_to(dataset):
                slugs.remove(dataset.slug)
                continue

            if not dry_run:
                uri = record.latest_uri

                # NOTE: set extract to false if there are any archives present in the dataset
                extract = True
                for f in dataset.files:
                    try:
                        patoolib.get_archive_format(f.entity.path)
                    except patoolib.util.PatoolError:
                        continue
                    else:
                        extract = False
                        break

                import_dataset(
                    uri=uri, slug=dataset.slug, extract=extract, yes=True, previous_dataset=dataset, delete=delete
                )

                communication.echo(f"Updated dataset '{dataset.slug}' from remote provider")

            slugs.remove(dataset.slug)
            imported_dataset_updates.append(dataset)
    else:
        must_match_records = True

    imported_dataset_updates_view_models = [DatasetViewModel.from_dataset(d) for d in imported_dataset_updates]

    if not slugs:
        return imported_dataset_updates_view_models, []

    # NOTE: Exclude all imported dataset from individual file filter
    records = filter_dataset_files(
        slugs=slugs,
        creators=creators,
        include=include,
        exclude=exclude,
        ignore=[d.slug for d in imported_datasets],
        check_data_directory=check_data_directory,
    )

    if not records:
        if must_match_records and not plain:
            raise errors.ParameterError("No files matched the criteria.")
        return imported_dataset_updates_view_models, []

    provider_files: Dict[AddProviderInterface, List[DynamicProxy]] = defaultdict(list)
    unique_remotes = set()
    linked_files = []

    for file in records:
        if file.linked:
            linked_files.append(file)
        else:
            if not getattr(file, "provider", None):
                if file.based_on:
                    uri = file.dataset.same_as.value if file.dataset.same_as else file.based_on.url
                else:
                    uri = file.source
                try:
                    file.provider = cast(
                        AddProviderInterface,
                        ProviderFactory.get_add_provider(uri),
                    )
                except errors.DatasetProviderNotFound:
                    communication.warn(f"Couldn't find provider for file {file.path} in dataset {file.dataset.slug}")
                    continue

            provider_files[file.provider].append(file)

            if isinstance(file.provider, GitProvider):
                unique_remotes.add(file.based_on.url)

    if ref and len(unique_remotes) > 1:
        raise errors.ParameterError(
            "Cannot specify a reference with more than one Git repository.\n"
            "Limit list of files to be updated to one repository. See 'renku dataset update -h' for more information."
        )

    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []

    if linked_files:
        updated = update_linked_files(linked_files, dry_run=dry_run)
        updated_files.extend(updated)

    provider_context: Dict[str, Any] = {}

    for provider, files in provider_files.items():
        if (no_remote and cast(ProviderApi, provider).is_remote) or (
            no_local and not cast(ProviderApi, provider).is_remote
        ):
            continue

        results = provider.update_files(
            files=files,
            dry_run=dry_run,
            delete=delete,
            context=provider_context,
            ref=ref,
            check_data_directory=check_data_directory,
        )
        updated_files.extend(r.entity for r in results if r.action == DatasetUpdateAction.UPDATE)
        deleted_files.extend(r.entity for r in results if r.action == DatasetUpdateAction.DELETE)

    if not dry_run:
        if deleted_files and not delete:
            communication.echo("Some files are deleted: Pass '--delete' to remove them from datasets' metadata")
        if updated_files or (deleted_files and delete):
            file_paths = {str(project_context.path / f.entity.path) for f in updated_files}
            # Force-add to include possible ignored files that are in datasets
            repository = project_context.repository
            repository.add(*file_paths, force=True)
            repository.add(project_context.pointers_path, force=True)

            _update_datasets_files_metadata(updated_files, deleted_files, delete)

        message = f"Updated {len(updated_files)} files"
        if delete:
            message += f" and deleted {len(deleted_files)} files"
        communication.echo(message)
    else:
        for file in deleted_files:
            if not file.date_removed:
                file.date_removed = local_now()

    dataset_files_view_models = [
        DatasetFileViewModel.from_dataset_file(cast(DatasetFile, f), f.dataset) for f in updated_files + deleted_files
    ]
    return imported_dataset_updates_view_models, dataset_files_view_models


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def show_dataset(slug: str, tag: Optional[str] = None):
    """Show detailed dataset information.

    Args:
        slug(str): Slug of dataset to show details for.
        tag(str, optional): Tags for which to get the metadata (Default value = None).

    Returns:
        dict: JSON dictionary of dataset details.
    """
    datasets_provenance = DatasetsProvenance()
    dataset: Optional[Dataset] = datasets_provenance.get_by_slug(slug, strict=True)

    if tag is None:
        return DatasetDetailsJson().dump(dataset)

    tags = datasets_provenance.get_all_tags(dataset=cast(Dataset, dataset))

    selected_tag = next((t for t in tags if t.name == tag), None)

    if selected_tag is None:
        raise errors.DatasetTagNotFound(tag)

    dataset = datasets_provenance.get_by_id(selected_tag.dataset_id.value)
    return DatasetDetailsJson().dump(dataset)


def add_datadir_files_to_dataset(dataset: Dataset) -> None:
    """Add all files in a datasets data directory to the dataset.

    Args:
        dataset(Dataset): The dataset to add data dir files to.
    """
    datadir = get_safe_relative_path(dataset.get_datadir(), project_context.path)

    if datadir.exists():
        # NOTE: Add existing files to dataset
        dataset_files: List[DatasetFile] = []
        files: List[Path] = []
        existing_files: List[Union[Path, str]] = list(get_files(datadir))
        checksums = project_context.repository.get_object_hashes(existing_files)

        for file in cast(List[Path], existing_files):
            files.append(file)
            dataset_files.append(DatasetFile.from_path(path=file, source=file, checksum=checksums.get(file)))

        if not dataset_files:
            return

        if check_external_storage():
            track_paths_in_storage(*files)
        project_context.repository.add(*files)

        dataset.add_or_update_files(dataset_files)


def set_dataset_images(dataset: Dataset, images: Optional[List[ImageObjectRequest]]):
    """Set a dataset's images.

    Args:
        dataset(Dataset): The dataset to set images on.
        images(List[ImageObjectRequest]): The images to set.

    Returns:
        True if images were set/modified.
    """
    if not images:
        images = []

    image_folder = project_context.dataset_images_path / dataset.initial_identifier
    image_folder.mkdir(exist_ok=True, parents=True)
    previous_images = dataset.images or []

    dataset.images = []
    images_updated = False
    for img in images:
        image_folder = project_context.dataset_images_path / dataset.initial_identifier
        try:
            img_object = img.to_image_object(owner_id=dataset.id)
        except errors.ImageError as e:
            raise errors.DatasetImageError(e) from e

        path = img_object.content_url

        if not img_object.is_remote:
            # NOTE: only copy dataset image if it's not in .renku/datasets/<id>/images/ already
            if not path.startswith(str(image_folder)):
                image_type = filetype.guess(path)
                if image_type:
                    ext = f".{image_type.extension}"
                else:
                    _, ext = os.path.splitext(path)
                target_image_path: Union[Path, str] = image_folder / f"{img_object.position}{ext}"

                image_folder.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(path, target_image_path)
            else:
                target_image_path = path

            img_object.content_url = get_relative_path(target_image_path, base=project_context.path)  # type: ignore

        if any(i.position == img_object.position for i in dataset.images):
            raise errors.DatasetImageError(f"Duplicate dataset image specified for position {img_object.position}")

        dataset.images.append(img_object)
        images_updated = True

    new_urls = [i.content_url for i in dataset.images]

    for prev in previous_images:
        # NOTE: Delete images if they were removed
        if prev.content_url in new_urls or urllib.parse.urlparse(prev.content_url).netloc:
            continue

        path = prev.content_url
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(project_context.path, path))

        os.remove(path)

    return images_updated or dataset.images != previous_images


def update_dataset_custom_metadata(
    dataset: Dataset,
    custom_metadata: Optional[Union[Dict, List[Dict]]],
    custom_metadata_source: Optional[str],
):
    """Update custom metadata on a dataset.

    Args:
        dataset(Dataset): The dataset to update.
        custom_metadata(Dict): Custom JSON-LD metadata to set.
        custom_metadata_source(str): The source field for the custom metadata.
    """

    existing_metadata = [a for a in dataset.annotations if a.source != custom_metadata_source]

    if custom_metadata is not None and custom_metadata_source is not None:
        if isinstance(custom_metadata, dict):
            custom_metadata = [custom_metadata]
        for cm in custom_metadata:
            existing_metadata.append(Annotation(id=Annotation.generate_id(), body=cm, source=custom_metadata_source))

    dataset.annotations = existing_metadata


@inject.autoparams("dataset_gateway")
def move_files(dataset_gateway: IDatasetGateway, files: Dict[Path, Path], to_dataset_slug: Optional[str] = None):
    """Move files and their metadata from one or more datasets to a target dataset.

    Args:
        dataset_gateway(IDatasetGateway):Injected dataset gateway.
        files(Dict[Path, Path]): Files to move
        to_dataset_slug(Optional[str], optional): Target dataset (Default value = None)
    """
    datasets = [d.copy() for d in dataset_gateway.get_all_active_datasets()]

    to_dataset: Optional[Dataset] = None
    if to_dataset_slug:
        # NOTE: Use the same dataset object or otherwise a race happens if dataset is in both source and destination
        to_dataset = next(d for d in datasets if d.slug == to_dataset_slug)
    modified_datasets: Dict[str, Dataset] = {}

    progress_name = "Updating dataset metadata"
    communication.start_progress(progress_name, total=len(files))
    try:
        checksums = project_context.repository.get_object_hashes(
            [file.relative_to(project_context.path) for file in files.values()]
        )
        for src, dst in files.items():
            src = src.relative_to(project_context.path)
            dst = dst.relative_to(project_context.path)
            # NOTE: Files are moved at this point, so, we can use dst
            new_dataset_file = DatasetFile.from_path(dst, checksum=checksums.get(dst))

            for dataset in datasets:
                removed = dataset.unlink_file(src, missing_ok=True)
                if removed:
                    modified_datasets[dataset.slug] = dataset
                    new_dataset_file.based_on = removed.based_on
                    new_dataset_file.source = removed.source

                    if not to_dataset and (
                        new_dataset_file.linked
                        or is_subpath(project_context.path / dst, project_context.path / dataset.get_datadir())
                    ):
                        dataset.add_or_update_files(new_dataset_file)

                # NOTE: Update dataset if it contains a destination that is being overwritten
                modified = dataset.find_file(dst)
                added = is_subpath(project_context.path / dst, project_context.path / dataset.get_datadir())
                if modified or added:
                    modified_datasets[dataset.slug] = dataset
                    dataset.add_or_update_files(new_dataset_file)

            if to_dataset:
                to_dataset.add_or_update_files(new_dataset_file)

            communication.update_progress(progress_name, amount=1)
    finally:
        communication.finalize_progress(progress_name)

    datasets_provenance = DatasetsProvenance()
    modified_dataset_values = list(modified_datasets.values())
    creator = get_git_user(repository=project_context.repository)
    for modified_dataset in modified_dataset_values:
        datasets_provenance.add_or_update(modified_dataset, creator=creator)
    if to_dataset and to_dataset not in modified_dataset_values:
        datasets_provenance.add_or_update(to_dataset, creator=creator)


def _update_datasets_files_metadata(updated_files: List[DynamicProxy], deleted_files: List[DynamicProxy], delete: bool):
    modified_datasets = {}
    checksums = project_context.repository.get_object_hashes([file.entity.path for file in updated_files])
    for file in updated_files:
        new_file = DatasetFile.from_path(
            path=file.entity.path, based_on=file.based_on, source=file.source, checksum=checksums.get(file.entity.path)
        )
        modified_datasets[file.dataset.slug] = (
            file.dataset._subject if isinstance(file.dataset, DynamicProxy) else file.dataset
        )
        file.dataset.add_or_update_files(new_file)

    if delete:
        for file in deleted_files:
            modified_datasets[file.dataset.slug] = (
                file.dataset._subject if isinstance(file.dataset, DynamicProxy) else file.dataset
            )
            file.dataset.unlink_file(file.entity.path)

    datasets_provenance = DatasetsProvenance()
    for dataset in modified_datasets.values():
        datasets_provenance.add_or_update(dataset, creator=get_git_user(repository=project_context.repository))


def update_linked_files(records: List[DynamicProxy], dry_run: bool) -> List[DynamicProxy]:
    """Update files linked to other files in the project.

    Args:
        records(List[DynamicProxy]): File records to update.
        dry_run(bool): Whether to return a preview of what would be updated.
    """
    updated_files = []

    for file in records:
        if file.linked:
            try:
                updated, checksum = is_linked_file_updated(path=file.entity.path)
            except errors.ExternalFileNotFound as e:
                if not dry_run:
                    raise
                communication.warn(str(e))
                continue

            if updated:
                if not dry_run:
                    update_linked_file(path=file.entity.path, checksum=checksum)
                updated_files.append(file)

    return updated_files


@inject.autoparams("dataset_gateway")
def filter_dataset_files(
    dataset_gateway: IDatasetGateway,
    slugs: Optional[List[str]] = None,
    tag: Optional[str] = None,
    creators: Optional[Union[str, List[str], Tuple[str]]] = None,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    ignore: Optional[List[str]] = None,
    immutable: bool = False,
    check_data_directory: bool = False,
) -> List[DynamicProxy]:
    """Filter dataset files by specified filters.

    Args:
        dataset_gateway(IDatasetGateway):Injected dataset gateway.
        slugs(Optional[List[str]]): Filter by specified dataset slugs (Default value = None).
        tag(Optional[str]): Filter by specified tag (Default value = None).
        creators(Optional[Union[str, List[str], Tuple[str]]]): Filter by creators (Default value = None).
        include(Optional[List[str]]): Tuple containing patterns to which include from result (Default value = None).
        exclude(Optional[List[str]]): Tuple containing patterns to which exclude from result (Default value = None).
        ignore(Optional[List[str]]): Ignored datasets (Default value = None).
        immutable(bool): Return immutable copies of dataset objects (Default value = False).
        check_data_directory(bool): Whether to check for new files in dataset's data directory that aren't in the
            dataset yet (Default value = False).
    Returns:
        List[DynamicProxy]: List of filtered files sorted by date added.
    """

    def should_include(filepath: Path) -> bool:
        """Check if file matches one of include filters and not in exclude filter."""
        if exclude:
            for pattern in exclude:
                if filepath.match(pattern):
                    return False

        if include:
            for pattern in include:
                if filepath.match(pattern):
                    return True
            return False

        return True

    creators_set = set()
    if isinstance(creators, str):
        creators_set = set(creators.split(","))
    elif isinstance(creators, list) or isinstance(creators, tuple):
        creators_set = set(creators)

    records = []
    unused_slugs = set(slugs) if slugs is not None else set()

    if ignore:
        unused_slugs = unused_slugs - set(ignore)

    for dataset in dataset_gateway.get_all_active_datasets():
        if (slugs and dataset.slug not in slugs) or (ignore and dataset.slug in ignore):
            continue

        if tag:
            dataset = get_dataset_by_tag(dataset=dataset, tag=tag)  # type: ignore
            if not dataset:
                continue

        if not immutable:
            dataset = dataset.copy()

        if unused_slugs:
            unused_slugs.remove(dataset.slug)

        if creators_set:
            dataset_creators = {creator.name for creator in dataset.creators}
            if not creators_set.issubset(dataset_creators):
                continue

        for file in dataset.files:
            if not should_include(Path(file.entity.path)):
                continue

            record = DynamicProxy(file)
            record.dataset = DynamicProxy(dataset)
            records.append(record)

        if not check_data_directory:
            continue

        for root, _, files in os.walk(project_context.path / dataset.get_datadir()):
            current_folder = Path(root)
            for current_file in files:
                file_path = get_safe_relative_path(current_folder / current_file, project_context.path)
                if should_include(file_path) and not dataset.find_file(file_path):
                    # New file in dataset folder
                    record = DynamicProxy(DatasetFile.from_path(file_path))
                    record.dataset = dataset
                    records.append(record)

    if unused_slugs:
        unused_slugs_str = ", ".join(unused_slugs)
        raise errors.ParameterError(f"These datasets don't exist: {unused_slugs_str}")

    return sorted(records, key=lambda r: r.date_added)


def download_file(file: DatasetFile, storage: "IStorage") -> List[DatasetFile]:
    """Download a dataset file and retrieve its missing metadata (if any).

    Args:
        file(DatasetFile): Dataset file to download.
        storage: Dataset's cloud storage (an instance of ``IStorage``).

    Returns:
         List[DatasetFile]: A list with the updated file if its metadata was missing; an empty list otherwise.

    """
    if not file.based_on:
        raise errors.DatasetImportError(f"Dataset file doesn't have a URI: {file.entity.path}")

    path = project_context.path / file.entity.path
    path.resolve().parent.mkdir(parents=True, exist_ok=True)

    # NOTE: Don't check if destination file exists. ``IStorage.copy`` won't copy a file if it exists and is not
    # modified.

    communication.start_progress(name=file.entity.path, total=1)
    try:
        storage.download(file.based_on.url, path)
        communication.update_progress(name=file.entity.path, amount=1)
    finally:
        communication.finalize_progress(name=file.entity.path)

    # NOTE: File has no missing information
    if file.has_valid_checksum() and file.has_valid_size():
        return []

    if not file.has_valid_checksum():
        md5_hash = hash_file(path, hash_type="md5") or NON_EXISTING_ENTITY_CHECKSUM
        entity = Entity(path=file.entity.path, checksum=md5_hash)
        remote_entity = RemoteEntity(checksum=md5_hash, url=file.based_on.url, path=file.based_on.path)
    else:
        entity = file.entity
        remote_entity = file.based_on

    size = file.size if file.has_valid_size() else get_file_size(path)

    return [
        DatasetFile(
            entity=entity,
            based_on=remote_entity,
            size=size,
            date_added=file.date_added,
            date_removed=file.date_removed,
            source=file.source,
        )
    ]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def pull_cloud_storage(slug: str, location: Optional[Path] = None) -> None:
    """Pull/copy data for a cloud storage to a dataset's data directory or a specified location.

    Args:
        slug(str): Slug of the dataset
        location(Optional[Path]): A directory to copy data to (Default value = None).
    """
    dataset, datadir = _get_dataset_with_cloud_storage(slug=slug)

    # NOTE: Try to unmount the path in case it was mounted before
    unmount_path(datadir)

    if location:
        if not is_path_empty(datadir):
            communication.confirm(
                f"Dataset's data directory will be removed: {dataset.get_datadir()}. Do you want to continue?",
                abort=True,
                warning=True,
            )
        create_symlink(target=location, symlink_path=datadir, overwrite=True)

    provider = ProviderFactory.get_pull_provider(uri=dataset.storage)
    storage = provider.get_storage()

    updated_files = parallel_execute(download_file, dataset.files, rate=5, storage=storage)

    if updated_files:
        dataset.add_or_update_files(updated_files)
        DatasetsProvenance().add_or_update(dataset, creator=get_git_user(repository=project_context.repository))
        project_context.database.commit()


def store_dataset_data_location(dataset: Dataset, location: Optional[Path]) -> None:
    """Store data location for a dataset in the config file."""
    section = "dataset-locations"
    key = dataset.slug

    if not location:
        remove_value(section=section, key=key)
    else:
        set_value(section=section, key=key, value=get_absolute_path(location))


def read_dataset_data_location(dataset: Dataset) -> Optional[str]:
    """Read data location for a dataset in the config file."""
    return get_value(section="dataset-locations", key=dataset.slug, config_filter=ConfigFilter.LOCAL_ONLY)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def mount_cloud_storage(slug: str, existing: Optional[Path], yes: bool) -> None:
    """Mount a cloud storage to a dataset's data directory.

    Args:
        slug(str): Slug of the dataset
        existing(Optional[Path]): An existing mount point to use instead of actually mounting the backend storage.
        yes(bool): Don't prompt when removing non-empty dataset's data directory.
    """
    dataset, datadir = _get_dataset_with_cloud_storage(slug=slug)

    # NOTE: Try to unmount the path in case it was mounted before
    unmount_path(datadir)

    if not is_path_empty(datadir) and not yes:
        communication.confirm(
            f"Dataset's data directory will be removed: {dataset.get_datadir()}. Do you want to continue?",
            abort=True,
            warning=True,
        )

    if existing:
        create_symlink(target=existing, symlink_path=datadir, overwrite=True)
        return

    delete_path(datadir)
    datadir.mkdir(parents=True, exist_ok=True)

    provider = ProviderFactory.get_mount_provider(uri=dataset.storage)
    credentials = provider.get_credentials()
    prompt_for_credentials(credentials)
    storage = provider.get_storage(credentials=credentials)

    with communication.busy(f"Mounting {provider.uri}"):
        storage.mount(datadir)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def unmount_cloud_storage(slug: str) -> None:
    """Mount a cloud storage to a dataset's data directory.

    Args:
        slug(str): Slug of the dataset
    """
    _, datadir = _get_dataset_with_cloud_storage(slug=slug)
    unmount_path(datadir)


def _get_dataset_with_cloud_storage(slug: str) -> Tuple[Dataset, Path]:
    datasets_provenance = DatasetsProvenance()

    dataset = datasets_provenance.get_by_slug(slug=slug, strict=True)

    if not dataset.storage:
        raise errors.ParameterError(f"Dataset '{slug}' doesn't have a storage backend")

    datadir = project_context.path / dataset.get_datadir()

    return dataset, datadir
