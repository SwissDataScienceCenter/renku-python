# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Dataset business logic."""

import os
import shutil
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

import patoolib
from pydantic import validate_arguments

from renku.command.command_builder.command import inject
from renku.command.view_model.dataset import DatasetFileViewModel, DatasetViewModel
from renku.core import errors
from renku.core.config import get_value, remove_value, set_value
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file, is_external_file_updated, update_external_file
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.models import ProviderDataset
from renku.core.dataset.providers.s3 import S3Credentials
from renku.core.dataset.request_model import ImageRequestModel
from renku.core.dataset.tag import get_dataset_by_tag, prompt_access_token, prompt_tag_selection
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.storage import check_external_storage, pull_paths_from_storage, track_paths_in_storage
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.git import clone_repository, get_cache_directory_for_repository, get_git_user
from renku.core.util.metadata import is_external_file, prompt_for_credentials, read_credentials, store_credentials
from renku.core.util.os import (
    create_symlink,
    delete_dataset_file,
    delete_path,
    get_absolute_path,
    get_files,
    get_safe_relative_path,
    hash_file,
    is_path_empty,
    is_subpath,
    unmount_path,
)
from renku.core.util.tabulate import tabulate
from renku.core.util.urls import get_slug
from renku.core.util.util import NO_VALUE, NoValueType
from renku.domain_model.dataset import Dataset, DatasetDetailsJson, DatasetFile, RemoteEntity, is_dataset_name_valid
from renku.domain_model.enums import ConfigFilter
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.infrastructure.repository import Repository


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def search_datasets(name: str) -> List[str]:
    """Get all the datasets whose name starts with the given string.

    Args:
        name(str): Beginning of dataset name to search for.

    Returns:
        List[str]: List of found dataset names.
    """
    datasets_provenance = DatasetsProvenance()
    return list(filter(lambda x: x.startswith(name), map(lambda x: x.name, datasets_provenance.datasets)))


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


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_dataset(
    name: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
    creators: Optional[List[Person]] = None,
    keywords: Optional[List[str]] = None,
    images: Optional[List[ImageRequestModel]] = None,
    update_provenance: bool = True,
    custom_metadata: Optional[Dict[str, Any]] = None,
    storage: Optional[str] = None,
    datadir: Optional[Path] = None,
):
    """Create a dataset.

    Args:
        name(str): Name of the dataset
        title(Optional[str], optional): Dataset title (Default value = None).
        description(Optional[str], optional): Dataset description (Default value = None).
        creators(Optional[List[Person]], optional): Dataset creators (Default value = None).
        keywords(Optional[List[str]], optional): Dataset keywords (Default value = None).
        images(Optional[List[ImageRequestModel]], optional): Dataset images (Default value = None).
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

    if not is_dataset_name_valid(name):
        valid_name = get_slug(name, lowercase=False)
        raise errors.ParameterError(f"Dataset name '{name}' is not valid (Hint: '{valid_name}' is valid).")

    datasets_provenance = DatasetsProvenance()

    if datasets_provenance.get_by_name(name=name):
        raise errors.DatasetExistsError(name)

    if not title:
        title = name

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
        name=name,
        title=title,
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


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def edit_dataset(
    name: str,
    title: Optional[Union[str, NoValueType]],
    description: Optional[Union[str, NoValueType]],
    creators: Optional[Union[List[Person], NoValueType]],
    keywords: Optional[Union[List[str], NoValueType]] = NO_VALUE,
    images: Optional[Union[List[ImageRequestModel], NoValueType]] = NO_VALUE,
    custom_metadata: Optional[Union[Dict, List[Dict], NoValueType]] = NO_VALUE,
    custom_metadata_source: Optional[Union[str, NoValueType]] = NO_VALUE,
):
    """Edit dataset metadata.

    Args:
        name(str): Name of the dataset to edit
        title(Optional[Union[str, NoValueType]]): New title for the dataset.
        description(Optional[Union[str, NoValueType]]): New description for the dataset.
        creators(Optional[Union[List[Person], NoValueType]]): New creators for the dataset.
        keywords(Optional[Union[List[str], NoValueType]]): New keywords for dataset (Default value = ``NO_VALUE``).
        images(Optional[Union[List[ImageRequestModel], NoValueType]]): New images for dataset
            (Default value = ``NO_VALUE``).
        custom_metadata(Optional[Union[Dict, List[Dict], NoValueType]]): Custom JSON-LD metadata
            (Default value = ``NO_VALUE``).
        custom_metadata_source(Optional[Union[str, NoValueType]]): The custom metadata source
            (Default value = ``NO_VALUE``).

    Returns:
        bool: True if updates were performed.
    """
    if isinstance(title, str):
        title = title.strip()

    if title is None:
        title = ""

    possible_updates = {
        "creators": creators,
        "description": description,
        "keywords": keywords,
        "title": title,
    }

    dataset_provenance = DatasetsProvenance()
    dataset = dataset_provenance.get_by_name(name=name)

    if dataset is None:
        raise errors.ParameterError("Dataset does not exist.")

    updated: Dict[str, Any] = {k: v for k, v in possible_updates.items() if v != NO_VALUE}

    if updated:
        dataset.update_metadata(creators=creators, description=description, keywords=keywords, title=title)

    if images == NO_VALUE:
        images_updated = False
    else:
        images_updated = set_dataset_images(dataset=dataset, images=cast(Optional[List[ImageRequestModel]], images))

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


@validate_arguments(config=dict(arbitrary_types_allowed=True))
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
        names=datasets, tag=tag, creators=creators, include=include, exclude=exclude, immutable=True
    )
    for record in records:
        record.title = record.dataset.title
        record.dataset_name = record.dataset.name
        record.dataset_id = record.dataset.id
        record.creators_csv = record.dataset.creators_csv
        record.creators_full_csv = record.dataset.creators_full_csv
        record.full_path = project_context.path / record.entity.path
        record.path = record.entity.path
        record.name = Path(record.entity.path).name
        record.added = record.date_added

    get_lfs_tracking_and_file_sizes(records, has_tag=bool(tag))

    return records


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def file_unlink(name: str, include: Optional[List[str]], exclude: Optional[List[str]], yes: bool = False):
    """Remove matching files from a dataset.

    Args:
        name(str): Dataset name.
        include(Optional[List[str]]): Include filter for files.
        exclude(Optional[List[str]]): Exclude filter for files.
        yes(bool): Whether to skip user confirmation or not (Default value = False).

    Returns:
        List[DynamicProxy]: List of files that were removed.
    """
    repository = project_context.repository

    if not include and not exclude:
        raise errors.ParameterError("include or exclude filters not specified.")

    datasets_provenance = DatasetsProvenance()

    dataset = datasets_provenance.get_by_name(name=name)

    if not dataset:
        raise errors.ParameterError("Dataset does not exist.")

    records = filter_dataset_files(names=[name], include=include, exclude=exclude)
    if not records:
        raise errors.ParameterError("No records found.")

    if not yes:
        prompt_text = (
            f'You are about to remove following from "{name}" dataset.'
            + "\n"
            + "\n".join([str(record.entity.path) for record in records])
            + "\nDo you wish to continue?"
        )
        communication.confirm(prompt_text, abort=True, warning=True)

    dataset_datadir = dataset.get_datadir()
    for file in records:
        dataset.unlink_file(file.entity.path)
        path_file = Path(file.entity.path)
        # INFO: Remove actual dataset file only if it is located within dataset directory
        if str(path_file.absolute()).startswith(str(dataset_datadir.absolute())):
            if not path_file.exists():
                communication.warn(
                    f"Dataset file {path_file} could not be found, skipping the removal from {dataset_datadir}."
                )
                continue
            try:
                if path_file.is_dir():
                    shutil.rmtree(str(path_file.absolute()), ignore_errors=False, onerror=None)
                else:
                    path_file.unlink()
            except Exception as err:
                communication.warn(
                    f"Dataset file {path_file} could not be removed from {dataset_datadir} because of {err}."
                )
            else:
                repository.add(path_file)

    datasets_provenance.add_or_update(dataset, creator=get_git_user(repository))

    return records


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def remove_dataset(name: str):
    """Delete a dataset.

    Args:
        name(str): Name of dataset to delete.
    """
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(name=name, strict=True)
    datasets_provenance.remove(dataset=dataset)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def export_dataset(name: str, provider_name: str, tag: Optional[str], **kwargs):
    """Export data to 3rd party provider.

    Args:
        name(str): Name of dataset to export.
        provider_name(str): Provider to use for export.
        tag(str): Dataset tag from which to export.
    """
    datasets_provenance = DatasetsProvenance()

    provider_name = provider_name.lower()

    # TODO: all these callbacks are ugly, improve in #737
    config_key_secret = "access_token"

    dataset: Optional[Dataset] = datasets_provenance.get_by_name(name, strict=True, immutable=True)

    provider = ProviderFactory.get_export_provider(provider_name=provider_name)

    selected_tag = None
    tags = datasets_provenance.get_all_tags(dataset)  # type: ignore

    if tag:
        selected_tag = next((t for t in tags if t.name == tag), None)

        if not selected_tag:
            raise errors.ParameterError(f"Tag '{tag}' not found for dataset '{name}'")
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


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def import_dataset(
    uri: str,
    name: Optional[str] = "",
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
        name(str): Name to give imported dataset (Default value = "").
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

    name = name or provider_dataset.name

    new_dataset = add_to_dataset(
        dataset_name=name,
        urls=[],
        importer=importer,
        create=not previous_dataset,
        force=True,  # NOTE: Force-add to include any ignored files
        extract=extract,
        overwrite=True,
        total_size=calculate_total_size(importer.provider_dataset_files),
        clear_files_before=True,
        datadir=datadir,
    )

    new_dataset.update_metadata_from(provider_dataset)
    # NOTE: Remove derived_from because this is an updated and imported dataset and won't be a derivative
    new_dataset.derived_from = None

    remove_files(new_dataset)

    importer.tag_dataset(name)
    importer.copy_extra_metadata(new_dataset)

    project_context.database.commit()


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def update_datasets(
    names: List[str],
    creators: Optional[str],
    include: Optional[List[str]],
    exclude: Optional[List[str]],
    ref: Optional[str],
    delete: bool,
    no_external: bool,
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
        names(List[str]): Names of datasets to update.
        creators(Optional[str]): Creators to filter dataset files by.
        include(Optional[List[str]]): Include filter for paths to update.
        exclude(Optional[List[str]]): Exclude filter for paths to update.
        ref(Optional[str]): Git reference to use for update.
        delete(bool): Whether to delete files that don't exist on remote anymore.
        no_external(bool): Whether to exclude external files from the update.
        no_local(bool): Whether to exclude local files from the update.
        no_remote(bool): Whether to exclude remote files from the update.
        check_data_directory(bool): Whether to check the dataset's data directory for new files.
        update_all(bool): Whether to update all datasets.
        dry_run(bool): Whether to return a preview of what would be updated.
        plain(bool): Whether plain output should be produced.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
    """
    from renku.core.dataset.providers.renku import RenkuProvider

    if not update_all and not names and not include and not exclude and not dry_run:
        raise errors.ParameterError("No update criteria is specified")

    imported_dataset_updates: List[Dataset] = []

    all_datasets = dataset_gateway.get_all_active_datasets()
    imported_datasets = [d for d in all_datasets if d.same_as]

    if names and update_all:
        raise errors.ParameterError("Cannot pass dataset names when updating all datasets")
    elif (include or exclude) and update_all:
        raise errors.ParameterError("Cannot specify include and exclude filters when updating all datasets")
    elif (include or exclude) and names and any(d for d in imported_datasets if d.name in names):
        raise errors.IncompatibleParametersError(a="--include/--exclude", b="imported datasets")

    names = names or [d.name for d in all_datasets]

    # NOTE: update imported datasets
    if not include and not exclude and not no_remote:
        must_match_records = False

        for dataset in imported_datasets:
            if dataset.name not in names:
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
                        f"Skipped updating imported Renku dataset '{dataset.name}' with tag '{tag.name}'"
                    )
                    names.remove(dataset.name)
                    continue

            if record.is_latest_version() and record.is_version_equal_to(dataset):
                names.remove(dataset.name)
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
                    uri=uri, name=dataset.name, extract=extract, yes=True, previous_dataset=dataset, delete=delete
                )

                communication.echo(f"Updated dataset '{dataset.name}' from remote provider")

            names.remove(dataset.name)
            imported_dataset_updates.append(dataset)
    else:
        must_match_records = True

    imported_dataset_updates_view_models = [DatasetViewModel.from_dataset(d) for d in imported_dataset_updates]

    if not names:
        return imported_dataset_updates_view_models, []

    # NOTE: Exclude all imported dataset from individual file filter
    records = filter_dataset_files(
        names=names,
        creators=creators,
        include=include,
        exclude=exclude,
        ignore=[d.name for d in imported_datasets],
        check_data_directory=check_data_directory,
    )

    if not records:
        if must_match_records and not plain:
            raise errors.ParameterError("No files matched the criteria.")
        return imported_dataset_updates_view_models, []

    git_files = []
    unique_remotes = set()
    external_files = []
    local_files = []

    for file in records:
        if file.based_on:
            git_files.append(file)
            unique_remotes.add(file.based_on.url)
        elif file.is_external:
            external_files.append(file)
        else:
            local_files.append(file)

    if ref and len(unique_remotes) > 1:
        raise errors.ParameterError(
            "Cannot specify a reference with more than one Git repository.\n"
            "Limit list of files to be updated to one repository. See 'renku dataset update -h' for more information."
        )

    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []

    if external_files and not no_external:
        updated = update_external_files(external_files, dry_run=dry_run)
        updated_files.extend(updated)

    if git_files and not no_remote:
        updated, deleted = update_dataset_git_files(files=git_files, ref=ref, delete=delete, dry_run=dry_run)
        updated_files.extend(updated)
        deleted_files.extend(deleted)

    if local_files and not no_local:
        updated, deleted, new = update_dataset_local_files(
            records=local_files, check_data_directory=check_data_directory
        )
        updated_files.extend(updated)
        deleted_files.extend(deleted)
        updated_files.extend(new)

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


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def show_dataset(name: str, tag: Optional[str] = None):
    """Show detailed dataset information.

    Args:
        name(str): Name of dataset to show details for.
        tag(str, optional): Tags for which to get the metadata (Default value = None).

    Returns:
        dict: JSON dictionary of dataset details.
    """
    datasets_provenance = DatasetsProvenance()
    dataset: Optional[Dataset] = datasets_provenance.get_by_name(name, strict=True)

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
        for file in get_files(datadir):
            files.append(file)
            dataset_files.append(DatasetFile.from_path(path=file, source=file))

        if not dataset_files:
            return

        if check_external_storage():
            track_paths_in_storage(*files)
        project_context.repository.add(*files)

        dataset.add_or_update_files(dataset_files)


def set_dataset_images(dataset: Dataset, images: Optional[List[ImageRequestModel]]):
    """Set a dataset's images.

    Args:
        dataset(Dataset): The dataset to set images on.
        images(List[ImageRequestModel]): The images to set.

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
        img_object = img.to_image_object(dataset)

        if not img_object:
            continue

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
        for icustom_metadata in custom_metadata:
            existing_metadata.append(
                Annotation(id=Annotation.generate_id(), body=icustom_metadata, source=custom_metadata_source)
            )

    dataset.annotations = existing_metadata


@inject.autoparams("dataset_gateway")
def move_files(dataset_gateway: IDatasetGateway, files: Dict[Path, Path], to_dataset_name: Optional[str] = None):
    """Move files and their metadata from one or more datasets to a target dataset.

    Args:
        dataset_gateway(IDatasetGateway):Injected dataset gateway.
        files(Dict[Path, Path]): Files to move
        to_dataset_name(Optional[str], optional): Target dataset (Default value = None)
    """
    datasets = [d.copy() for d in dataset_gateway.get_all_active_datasets()]

    to_dataset: Optional[Dataset] = None
    if to_dataset_name:
        # NOTE: Use the same dataset object or otherwise a race happens if dataset is in both source and destination
        to_dataset = next(d for d in datasets if d.name == to_dataset_name)
    modified_datasets: Dict[str, Dataset] = {}

    progress_name = "Updating dataset metadata"
    communication.start_progress(progress_name, total=len(files))
    try:
        for src, dst in files.items():
            src = src.relative_to(project_context.path)
            dst = dst.relative_to(project_context.path)
            # NOTE: Files are moved at this point, so, we can use dst
            new_dataset_file = DatasetFile.from_path(dst)

            for dataset in datasets:
                removed = dataset.unlink_file(src, missing_ok=True)
                if removed:
                    modified_datasets[dataset.name] = dataset
                    new_dataset_file.based_on = removed.based_on
                    new_dataset_file.source = removed.source

                    if not to_dataset and (
                        new_dataset_file.is_external
                        or is_subpath(project_context.path / dst, project_context.path / dataset.get_datadir())
                    ):
                        dataset.add_or_update_files(new_dataset_file)

                # NOTE: Update dataset if it contains a destination that is being overwritten
                modified = dataset.find_file(dst)
                added = is_subpath(project_context.path / dst, project_context.path / dataset.get_datadir())
                if modified or added:
                    modified_datasets[dataset.name] = dataset
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


def update_dataset_local_files(
    records: List[DynamicProxy], check_data_directory: bool
) -> Tuple[List[DynamicProxy], List[DynamicProxy], List[DynamicProxy]]:
    """Update files metadata from the git history.

    Args:
        records(List[DynamicProxy]): File records to update.
        check_data_directory(bool): Whether to check the dataset's data directory for new files.
    Returns:
        Tuple[List[DynamicProxy], List[DynamicProxy]]: Tuple of updated and deleted file records.
    """
    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []
    new_files: List[DynamicProxy] = []
    progress_text = "Checking for local updates"

    try:
        communication.start_progress(progress_text, len(records))
        check_paths = []
        records_to_check = []

        for file in records:
            communication.update_progress(progress_text, 1)

            if file.based_on or file.is_external:
                continue

            if not (project_context.path / file.entity.path).exists():
                deleted_files.append(file)
                continue

            check_paths.append(file.entity.path)
            records_to_check.append(file)

        checksums = project_context.repository.get_object_hashes(check_paths)

        for file in records_to_check:
            current_checksum = checksums.get(file.entity.path)
            if not current_checksum:
                deleted_files.append(file)
            elif current_checksum != file.entity.checksum:
                updated_files.append(file)
            elif check_data_directory and not any(file.entity.path == f.entity.path for f in file.dataset.files):
                datadir = file.dataset.get_datadir()
                try:
                    get_safe_relative_path(file.entity.path, datadir)
                except ValueError:
                    continue

                new_files.append(file)
    finally:
        communication.finalize_progress(progress_text)

    return updated_files, deleted_files, new_files


def _update_datasets_files_metadata(updated_files: List[DynamicProxy], deleted_files: List[DynamicProxy], delete: bool):
    modified_datasets = {}

    for file in updated_files:
        new_file = DatasetFile.from_path(path=file.entity.path, based_on=file.based_on, source=file.source)
        modified_datasets[file.dataset.name] = file.dataset
        file.dataset.add_or_update_files(new_file)

    if delete:
        for file in deleted_files:
            modified_datasets[file.dataset.name] = file.dataset
            file.dataset.unlink_file(file.entity.path)

    datasets_provenance = DatasetsProvenance()
    for dataset in modified_datasets.values():
        datasets_provenance.add_or_update(dataset, creator=get_git_user(repository=project_context.repository))


def update_dataset_git_files(
    files: List[DynamicProxy], ref: Optional[str], delete: bool, dry_run: bool
) -> Tuple[List[DynamicProxy], List[DynamicProxy]]:
    """Update files and dataset metadata according to their remotes.

    Args:
        files(List[DynamicProxy]): List of files to be updated.
        ref(Optional[str]): Reference to use for update.
        delete(bool, optional): Indicates whether to delete files or not (Default value = False).
        dry_run(bool): Whether to perform update or only print changes.

    Returns:
        Tuple[List[DynamicProxy], List[DynamicProxy]]: Tuple of updated and deleted file records.
    """
    visited_repos: Dict[str, "Repository"] = {}
    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []

    progress_text = "Checking files for updates"

    try:
        communication.start_progress(progress_text, len(files))
        for file in files:
            communication.update_progress(progress_text, 1)
            if not file.based_on:
                continue

            based_on = file.based_on
            url = based_on.url
            if url in visited_repos:
                remote_repository = visited_repos[url]
            else:
                communication.echo(msg="Cloning remote repository...")
                path = get_cache_directory_for_repository(url=url)
                remote_repository = clone_repository(url=url, path=path, checkout_revision=ref)
                visited_repos[url] = remote_repository

            checksum = remote_repository.get_object_hash(path=based_on.path, revision="HEAD")
            found = checksum is not None
            changed = found and based_on.checksum != checksum

            src = remote_repository.path / based_on.path
            dst = project_context.metadata_path.parent / file.entity.path

            if not found:
                if not dry_run and delete:
                    delete_dataset_file(dst, follow_symlinks=True)
                    project_context.repository.add(dst, force=True)
                deleted_files.append(file)
            elif changed:
                if not dry_run:
                    # Fetch file if it is tracked by Git LFS
                    pull_paths_from_storage(remote_repository, remote_repository.path / based_on.path)
                    if is_external_file(path=src, project_path=remote_repository.path):
                        delete_dataset_file(dst, follow_symlinks=True)
                        create_external_file(target=src.resolve(), path=dst)
                    else:
                        shutil.copy(src, dst)
                    file.based_on = RemoteEntity(
                        checksum=checksum, path=based_on.path, url=based_on.url  # type: ignore
                    )
                updated_files.append(file)
    finally:
        communication.finalize_progress(progress_text)

    if not updated_files and (not delete or not deleted_files):
        # Nothing to commit or update
        return [], deleted_files

    return updated_files, deleted_files


def update_external_files(records: List[DynamicProxy], dry_run: bool) -> List[DynamicProxy]:
    """Update files linked to external storage.

    Args:
        records(List[DynamicProxy]): File records to update.
        dry_run(bool): Whether to return a preview of what would be updated.
    """
    updated_files = []

    for file in records:
        if file.is_external:
            try:
                updated, checksum = is_external_file_updated(project_path=project_context.path, path=file.entity.path)
            except errors.ExternalFileNotFound as e:
                if not dry_run:
                    raise
                communication.warn(str(e))
                continue

            if updated:
                if not dry_run:
                    update_external_file(path=file.entity.path, checksum=checksum)
                updated_files.append(file)

    return updated_files


@inject.autoparams("dataset_gateway")
def filter_dataset_files(
    dataset_gateway: IDatasetGateway,
    names: Optional[List[str]] = None,
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
        names(Optional[List[str]]): Filter by specified dataset names (Default value = None).
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

    if isinstance(creators, str):
        creators_set = set(creators.split(","))
    elif isinstance(creators, list) or isinstance(creators, tuple):
        creators_set = set(creators)
    else:
        creators_set = set()

    records = []
    unused_names = set(names) if names is not None else set()

    if ignore:
        unused_names = unused_names - set(ignore)

    for dataset in dataset_gateway.get_all_active_datasets():
        if (names and dataset.name not in names) or (ignore and dataset.name in ignore):
            continue

        if tag:
            dataset = get_dataset_by_tag(dataset=dataset, tag=tag)  # type: ignore
            if not dataset:
                continue

        if not immutable:
            dataset = dataset.copy()

        if unused_names:
            unused_names.remove(dataset.name)

        if creators_set:
            dataset_creators = {creator.name for creator in dataset.creators}
            if not creators_set.issubset(dataset_creators):
                continue

        for file in dataset.files:
            if not should_include(Path(file.entity.path)):
                continue

            record = DynamicProxy(file)
            record.dataset = dataset
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

    if unused_names:
        unused_names_str = ", ".join(unused_names)
        raise errors.ParameterError(f"These datasets don't exist: {unused_names_str}")

    return sorted(records, key=lambda r: r.date_added)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def pull_external_data(name: str, location: Optional[Path] = None) -> None:
    """Pull/copy data for an external storage to a dataset's data directory or a specified location.

    Args:
        name(str): Name of the dataset
        location(Optional[Path]): A directory to copy data to (Default value = None).
    """
    datasets_provenance = DatasetsProvenance()

    dataset = datasets_provenance.get_by_name(name=name, strict=True)

    if not dataset.storage:
        communication.warn(f"Dataset '{name}' doesn't have a storage backend")
        return

    # NOTE: Try to unmount the path in case it was mounted before
    unmount_path(project_context.path / dataset.get_datadir())

    create_symlinks = True
    destination: Union[Path, str]

    if location:
        destination = get_absolute_path(location)
    else:
        stored_location = read_dataset_data_location(dataset=dataset)
        if stored_location:
            destination = stored_location
        else:
            destination = project_context.path
            create_symlinks = False

    provider = ProviderFactory.get_pull_provider(uri=dataset.storage)
    storage = provider.get_storage()

    updated_files = []

    for file in dataset.files:
        path = Path(destination) / file.entity.path
        path.parent.mkdir(parents=True, exist_ok=True)
        # NOTE: Don't check if destination exists. ``IStorage.copy`` won't copy a file if it exists and is not modified.

        if not file.based_on:
            raise errors.DatasetImportError(f"Dataset file doesn't have a URI: {file.entity.path}")

        with communication.busy(f"Copying {file.entity.path} ..."):
            storage.download(file.based_on.url, path)

            # NOTE: Make files read-only since we don't support pushing data to the remote storage
            os.chmod(path, 0o400)

            if not file.based_on.checksum:
                md5_hash = hash_file(path, hash_type="md5") or ""
                file.based_on = RemoteEntity(checksum=md5_hash, url=file.based_on.url, path=file.based_on.path)

            new_file = DynamicProxy(file)
            new_file.dataset = dataset
            updated_files.append(new_file)

            if create_symlinks:
                symlink_path = project_context.path / file.entity.path
                symlink_path.parent.mkdir(parents=True, exist_ok=True)
                create_symlink(path=path, symlink_path=symlink_path, overwrite=True)

    # NOTE: Store location in metadata in case where we want to mount the external storage in the same location
    store_dataset_data_location(dataset=dataset, location=location)

    if updated_files:
        _update_datasets_files_metadata(updated_files=updated_files, deleted_files=[], delete=False)


def store_dataset_data_location(dataset: Dataset, location: Optional[Path]) -> None:
    """Store data location for a dataset in the config file."""
    section = "dataset-locations"
    key = dataset.name

    if not location:
        remove_value(section=section, key=key)
    else:
        set_value(section=section, key=key, value=get_absolute_path(location))


def read_dataset_data_location(dataset: Dataset) -> Optional[str]:
    """Read data location for a dataset in the config file."""
    return get_value(section="dataset-locations", key=dataset.name, config_filter=ConfigFilter.LOCAL_ONLY)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def mount_external_storage(name: str, existing: Optional[Path], yes: bool) -> None:
    """Mount an external storage to a dataset's data directory.

    Args:
        name(str): Name of the dataset
        existing(Optional[Path]): An existing mount point to use instead of actually mounting the external storage.
        yes(bool): Don't prompt when removing non-empty dataset's data directory.
    """
    dataset, datadir = _get_dataset_with_external_storage(name=name)

    # NOTE: Try to unmount the path in case it was mounted before
    unmount_path(datadir)

    if not is_path_empty(datadir) and not yes:
        communication.confirm(
            f"Dataset's data directory will be removed: {dataset.get_datadir()}. Do you want to continue?",
            abort=True,
            warning=True,
        )

    if existing:
        create_symlink(path=existing, symlink_path=datadir, overwrite=True)
        return

    delete_path(datadir)
    datadir.mkdir(parents=True, exist_ok=True)

    provider = ProviderFactory.get_mount_provider(uri=dataset.storage)
    credentials = S3Credentials(provider)
    prompt_for_credentials(credentials)
    storage = provider.get_storage(credentials=credentials)

    with communication.busy(f"Mounting {provider.uri}"):
        storage.mount(datadir)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def unmount_external_storage(name: str) -> None:
    """Mount an external storage to a dataset's data directory.

    Args:
        name(str): Name of the dataset
    """
    _, datadir = _get_dataset_with_external_storage(name=name)
    unmount_path(datadir)


def _get_dataset_with_external_storage(name: str) -> Tuple[Dataset, Path]:
    datasets_provenance = DatasetsProvenance()

    dataset = datasets_provenance.get_by_name(name=name, strict=True)

    if not dataset.storage:
        raise errors.ParameterError(f"Dataset '{name}' doesn't have a storage backend")

    datadir = project_context.path / dataset.get_datadir()

    return dataset, datadir
