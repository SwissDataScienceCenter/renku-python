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
import re
import shutil
import urllib
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

import patoolib

from renku.command.command_builder.command import inject
from renku.command.view_model.dataset import DatasetFileViewModel, DatasetViewModel
from renku.core import errors
from renku.core.dataset.constant import renku_dataset_images_path, renku_pointers_path
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file, is_external_file_updated, update_external_file
from renku.core.dataset.providers import ProviderFactory
from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile
from renku.core.dataset.request_model import ImageRequestModel
from renku.core.dataset.tag import add_dataset_tag, prompt_access_token, prompt_tag_selection
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.doi import is_doi
from renku.core.util.git import clone_repository, get_cache_directory_for_repository, get_git_user
from renku.core.util.metadata import is_external_file
from renku.core.util.os import delete_file
from renku.core.util.urls import get_slug, remove_credentials
from renku.domain_model.dataset import (
    Dataset,
    DatasetDetailsJson,
    DatasetFile,
    RemoteEntity,
    Url,
    generate_default_name,
    get_dataset_data_dir,
    is_dataset_name_valid,
)
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.tabulate import tabulate
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.management.client import LocalClient
    from renku.infrastructure.repository import Repository


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
        dataset = DynamicProxy(dataset)
        dataset.tags = tags
        dataset.tags_csv = ",".join(tag.name for tag in tags)
        datasets.append(dataset)

    return list(datasets)


@inject.autoparams("client_dispatcher")
def create_dataset(
    name: str,
    client_dispatcher: IClientDispatcher,
    title: Optional[str] = None,
    description: Optional[str] = None,
    creators: Optional[List[Person]] = None,
    keywords: Optional[List[str]] = None,
    images: Optional[List[ImageRequestModel]] = None,
    update_provenance: bool = True,
    custom_metadata: Optional[Dict[str, Any]] = None,
):
    """Create a dataset.

    Args:
        name(str): Name of the dataset
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        title(Optional[str], optional): Dataset title (Default value = None).
        description(Optional[str], optional): Dataset description (Default value = None).
        creators(Optional[List[Person]], optional): Dataset creators (Default value = None).
        keywords(Optional[List[str]], optional): Dataset keywords (Default value = None).
        images(Optional[List[ImageRequestModel]], optional): Dataset images (Default value = None).
        update_provenance(bool, optional): Whether to add this dataset to dataset provenance
            (Default value = True).
        custom_metadata(Optional[Dict[str, Any]], optional): Custom JSON-LD metadata (Default value = None).

    Returns:
        Dataset: The created dataset.
    """
    client = client_dispatcher.current_client

    if not creators:
        creators = []
        user = get_git_user(client.repository)

        if user:
            creators.append(user)

    if not is_dataset_name_valid(name):
        valid_name = get_slug(name, lowercase=False)
        raise errors.ParameterError(f'Dataset name "{name}" is not valid (Hint: "{valid_name}" is valid).')

    datasets_provenance = DatasetsProvenance()

    if datasets_provenance.get_by_name(name=name):
        raise errors.DatasetExistsError(f"Dataset exists: '{name}'")

    if not title:
        title = name

    keywords = keywords or []

    annotations = None

    if custom_metadata:
        annotations = [Annotation(id=Annotation.generate_id(), source="renku", body=custom_metadata)]

    dataset = Dataset(
        identifier=None,
        name=name,
        title=title,
        description=description,
        creators=creators,
        keywords=keywords,
        project_id=client.project.id,
        annotations=annotations,
    )

    if images:
        set_dataset_images(client, dataset, images)

    if update_provenance:
        datasets_provenance.add_or_update(dataset)

    return dataset


@inject.autoparams("client_dispatcher")
def edit_dataset(
    name: str,
    title: str,
    description: str,
    creators: List[Person],
    client_dispatcher: IClientDispatcher,
    keywords: Optional[List[str]] = None,
    images: Optional[List[ImageRequestModel]] = None,
    skip_image_update: bool = False,
    custom_metadata: Optional[Dict] = None,
):
    """Edit dataset metadata.

    Args:
        name(str): Name of the dataset to edit
        title(str): New title for the dataset.
        description(str): New description for the dataset.
        creators(List[Person]): New creators for the dataset.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        keywords(List[str], optional): New keywords for dataset (Default value = None).
        images(List[ImageRequestModel], optional): New images for dataset (Default value = None).
        skip_image_update(bool, optional): Whether or not to skip updating dataset images (Default value = False).
        custom_metadata(Dict, optional): Custom JSON-LD metadata (Default value = None).

    Returns:
        bool: True if updates were performed.
    """
    client = client_dispatcher.current_client

    possible_updates = {
        "creators": creators,
        "description": description,
        "keywords": keywords,
        "title": title,
    }

    title = title.strip() if isinstance(title, str) else ""

    dataset_provenance = DatasetsProvenance()
    dataset = dataset_provenance.get_by_name(name=name)

    if dataset is None:
        raise errors.ParameterError("Dataset does not exist.")

    updated: Dict[str, Any] = {k: v for k, v in possible_updates.items() if v}

    if updated:
        dataset.update_metadata(creators=creators, description=description, keywords=keywords, title=title)

    if skip_image_update:
        images_updated = False
    else:
        images_updated = set_dataset_images(client, dataset, images)

    if images_updated:
        updated["images"] = [{"content_url": i.content_url, "position": i.position} for i in dataset.images]

    if custom_metadata:
        update_dataset_custom_metadata(dataset, custom_metadata)
        updated["custom_metadata"] = custom_metadata

    if not updated:
        return []

    datasets_provenance = DatasetsProvenance()
    datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))

    return updated


@inject.autoparams()
def list_dataset_files(
    client_dispatcher: IClientDispatcher,
    datasets=None,
    creators=None,
    include=None,
    exclude=None,
):
    """List dataset files.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        datasets: Datasets to list files for (Default value = None).
        creators: Creators to filter by (Default value = None).
        include: Include filters for file paths (Default value = None).
        exclude: Exclude filters for file paths (Default value = None).

    Returns:
        List[DynamicProxy]: Filtered dataset files.
    """
    from renku.command.format.dataset_files import get_lfs_file_sizes, get_lfs_tracking

    client = client_dispatcher.current_client

    records = filter_dataset_files(names=datasets, creators=creators, include=include, exclude=exclude, immutable=True)
    for record in records:
        record.title = record.dataset.title
        record.dataset_name = record.dataset.name
        record.dataset_id = record.dataset.id
        record.creators_csv = record.dataset.creators_csv
        record.creators_full_csv = record.dataset.creators_full_csv
        record.full_path = client.path / record.entity.path
        record.path = record.entity.path
        record.name = Path(record.entity.path).name
        record.added = record.date_added

    get_lfs_file_sizes(records)
    get_lfs_tracking(records)

    return records


@inject.autoparams()
def file_unlink(name, include, exclude, client_dispatcher: IClientDispatcher, yes=False):
    """Remove matching files from a dataset.

    Args:
        name: Dataset name.
        include: Include filter for files.
        exclude: Exclude filter for files.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        yes: Whether to skip user confirmation or not (Default value = False).

    Returns:
        List[DynamicProxy]: List of files that were removed.
    """
    client = client_dispatcher.current_client

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

    for file in records:
        dataset.unlink_file(file.entity.path)

    datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))

    return records


def remove_dataset(name):
    """Delete a dataset.

    Args:
        name: Name of dataset to delete.
    """
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(name=name, strict=True)
    datasets_provenance.remove(dataset=dataset)


@inject.autoparams()
def export_dataset(name, provider_name, publish, tag, client_dispatcher: IClientDispatcher, **kwargs):
    """Export data to 3rd party provider.

    Args:
        name: Name of dataset to export.
        provider_name: Provider to use for export.
        publish: Whether to export as proper version or draft.
        tag: Dataset tag from which to export.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
    """
    client = client_dispatcher.current_client
    datasets_provenance = DatasetsProvenance()

    provider_name = provider_name.lower()

    # TODO: all these callbacks are ugly, improve in #737
    config_key_secret = "access_token"

    dataset = datasets_provenance.get_by_name(name, strict=True, immutable=True)

    try:
        provider = ProviderFactory.from_id(provider_name)
    except KeyError:
        raise errors.ParameterError("Unknown provider.")

    provider.set_parameters(**kwargs)

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

    data_dir = get_dataset_data_dir(client, dataset)  # type: ignore
    dataset = cast(Dataset, DynamicProxy(dataset))
    dataset.data_dir = data_dir

    access_token = client.get_value(provider_name, config_key_secret)
    exporter = provider.get_exporter(dataset, access_token=access_token)

    if access_token is None:
        access_token = prompt_access_token(exporter)

        if access_token is None or len(access_token) == 0:
            raise errors.InvalidAccessToken()

        client.set_value(provider_name, config_key_secret, access_token, global_only=True)
        exporter.set_access_token(access_token)

    try:
        destination = exporter.export(publish=publish, tag=selected_tag, client=client)
    except errors.AuthenticationError:
        client.remove_value(provider_name, config_key_secret, global_only=True)
        raise

    communication.echo(f"Exported to: {destination}")


@inject.autoparams()
def import_dataset(
    uri,
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
    name="",
    extract=False,
    yes=False,
    previous_dataset=None,
    delete=False,
    gitlab_token=None,
):
    """Import data from a 3rd party provider or another renku project.

    Args:
        uri: DOI or URL of dataset to import.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        database_dispatcher(IDatabaseDispatcher): Injected database dispatcher.
        name: Name to give imported dataset (Default value = "").
        extract: Whether to extract compressed dataset data (Default value = False).
        yes: Whether to skip user confirmation (Default value = False).
        previous_dataset: Previously imported dataset version (Default value = None).
        delete: Whether to delete files that don't exist anymore (Default value = False).
        gitlab_token: Gitlab OAuth2 token (Default value = None).
    """
    from renku.core.dataset.dataset_add import add_data_to_dataset
    from renku.core.dataset.providers.renku import RenkuProvider, RenkuRecordSerializer

    client = client_dispatcher.current_client

    provider, err = ProviderFactory.from_uri(uri)
    if err and provider is None:
        raise errors.ParameterError(f"Could not process '{uri}'.\n{err}")

    assert provider is not None

    try:
        record = provider.find_record(uri, gitlab_token=gitlab_token)
        provider_dataset: ProviderDataset = record.as_dataset(client)
        files: List[ProviderDatasetFile] = record.files_info
        total_size = 0

        if not yes:
            communication.echo(
                tabulate(
                    files,
                    headers=OrderedDict(
                        (
                            ("checksum", "checksum"),
                            ("filename", "name"),
                            ("size_in_mb", "size (mb)"),
                            ("filetype", "type"),
                        )
                    ),
                    floatfmt=".2f",
                )
            )

            text_prompt = "Do you wish to download this version?"
            if not record.is_last_version(uri):
                text_prompt = f"Newer version found at {record.latest_uri}\n{text_prompt}"

            communication.confirm(text_prompt, abort=True, warning=True)

            for file_ in files:
                if file_.size_in_mb is not None:
                    total_size += file_.size_in_mb

            total_size *= 2**20

    except KeyError as e:
        raise errors.ParameterError(f"Could not process '{uri}'.\nUnable to fetch metadata: {e}")
    except LookupError as e:
        raise errors.ParameterError(f"Could not process '{uri}'.\nReason: {e}")

    if not files:
        raise errors.ParameterError(f"Dataset '{uri}' has no files.")

    new_dataset: Dataset

    if not isinstance(provider, RenkuProvider):
        if not name:
            name = generate_default_name(provider_dataset.title, provider_dataset.version)

        provider_dataset.same_as = Url(url_id=remove_credentials(uri))
        if is_doi(provider_dataset.identifier):
            provider_dataset.same_as = Url(url_str=urllib.parse.urljoin("https://doi.org", provider_dataset.identifier))

        urls, names = zip(*[(f.source, f.filename) for f in files])
        new_dataset = add_data_to_dataset(
            urls=urls,
            dataset_name=name,
            create=not previous_dataset,
            with_metadata=provider_dataset,
            force=True,
            extract=extract,
            all_at_once=True,
            destination_names=names,
            total_size=total_size,
            overwrite=True,
            clear_files_before=True,
        )

        if previous_dataset:
            new_dataset = _update_datasets_metadata(new_dataset, previous_dataset, delete, new_dataset.same_as)

        if provider_dataset.version:
            tag_name = re.sub("[^a-zA-Z0-9.-_]", "_", provider_dataset.version)
            add_dataset_tag(
                dataset_name=new_dataset.name,
                tag=tag_name,
                description=f"Tag {provider_dataset.version} created by renku import",
            )
    else:
        record = cast(RenkuRecordSerializer, record)
        name = name or provider_dataset.name

        provider_dataset.same_as = Url(url_id=record.latest_uri)

        if not provider_dataset.data_dir:
            raise errors.OperationError(f"Data directory for dataset must be set: {provider_dataset.name}")

        sources = []

        if record.datadir_exists:
            sources = [f"{provider_dataset.data_dir}/*"]

        for file in files:
            try:
                Path(file.path).relative_to(provider_dataset.data_dir)
            except ValueError:  # Files that are not in dataset's data directory
                sources.append(file.path)

        new_dataset = add_data_to_dataset(
            urls=[record.project_url],
            dataset_name=name,
            sources=sources,
            with_metadata=provider_dataset,
            create=not previous_dataset,
            overwrite=True,
            repository=record.repository,
            clear_files_before=True,
        )

        if previous_dataset:
            _update_datasets_metadata(new_dataset, previous_dataset, delete, provider_dataset.same_as)

        record.import_images(new_dataset)

    database_dispatcher.current_database.commit()


@inject.autoparams()
def update_datasets(
    names,
    creators,
    include,
    exclude,
    ref,
    delete,
    no_external,
    update_all,
    dry_run,
    client_dispatcher: IClientDispatcher,
    dataset_gateway: IDatasetGateway,
) -> Tuple[List[DatasetViewModel], List[DatasetFileViewModel]]:
    """Update dataset files.

    Args:
        names: Names of datasets to update.
        creators: Creators to filter dataset files by.
        include: Include filter for paths to update.
        exclude: Exclude filter for paths to update.
        ref: Git reference to use for update.
        delete: Whether to delete files that don't exist on remote anymore.
        no_external: Whether to exclude external files from the update.
        update_all: Whether to update all datasets.
        dry_run: Whether to return a preview of what would be updated.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
    """
    if not update_all and not names and not include and not exclude and not dry_run:
        raise errors.ParameterError("No update criteria is specified")

    client = client_dispatcher.current_client

    imported_datasets: List[Dataset] = []

    all_datasets = dataset_gateway.get_all_active_datasets()

    if names and update_all:
        raise errors.ParameterError("Cannot pass dataset names when updating all datasets")
    elif (include or exclude) and update_all:
        raise errors.ParameterError("Cannot specify include and exclude filters when updating all datasets")
    elif (include or exclude) and names and any(d.same_as for d in all_datasets if d.name in names):
        raise errors.IncompatibleParametersError(a="--include/--exclude", b="imported datasets")

    names_provided = bool(names)

    # NOTE: update imported datasets
    if not include and not exclude:
        for dataset in all_datasets:
            if names and dataset.name not in names or not dataset.same_as:
                continue

            uri = dataset.same_as.url
            if isinstance(uri, dict):
                uri = cast(str, uri.get("@id"))
            provider, _ = ProviderFactory.from_uri(uri)

            if not provider:
                continue

            record = provider.find_record(uri)

            if record.is_last_version(uri) and record.version == dataset.version:
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

            if names:
                names.remove(dataset.name)
            imported_datasets.append(dataset)
    else:
        imported_datasets = [d for d in all_datasets if d.same_as]

    imported_datasets_view_models = [DatasetViewModel.from_dataset(d) for d in imported_datasets]

    if names_provided and not names:
        return imported_datasets_view_models, []

    records = filter_dataset_files(
        names=names, creators=creators, include=include, exclude=exclude, ignore=[d.name for d in imported_datasets]
    )

    if not records:
        if imported_datasets:
            return imported_datasets_view_models, []
        raise errors.ParameterError("No files matched the criteria.")

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
        updated = update_external_files(client, external_files, dry_run=dry_run)
        updated_files.extend(updated)

    if git_files:
        updated, deleted = update_dataset_git_files(files=git_files, ref=ref, delete=delete, dry_run=dry_run)
        updated_files.extend(updated)
        deleted_files.extend(deleted)

    if local_files:
        updated, deleted = update_dataset_local_files(records=local_files)
        updated_files.extend(updated)
        deleted_files.extend(deleted)

    if not dry_run:
        if deleted_files and not delete:
            communication.echo("Some files are deleted: Pass '--delete' to remove them from datasets' metadata")
        if updated_files or (deleted_files and delete):
            file_paths = {str(client.path / f.entity.path) for f in updated_files}
            # Force-add to include possible ignored files that are in datasets
            client.repository.add(*file_paths, force=True)
            client.repository.add(renku_pointers_path(client), force=True)

            _update_datasets_files_metadata(client, updated_files, deleted_files, delete)

        message = f"Updated {len(updated_files)} files"
        if delete:
            message += f" and deleted {len(deleted_files)} files"
        communication.echo(message)
    else:
        for file in deleted_files:
            file.date_removed = local_now()

    dataset_files_view_models = [
        DatasetFileViewModel.from_dataset_file(cast(DatasetFile, f), f.dataset) for f in updated_files + deleted_files
    ]
    return imported_datasets_view_models, dataset_files_view_models


def show_dataset(name):
    """Show detailed dataset information.

    Args:
        name: Name of dataset to show details for.

    Returns:
        dict: JSON dictionary of dataset details.
    """
    dataset = DatasetsProvenance().get_by_name(name, strict=True)

    return DatasetDetailsJson().dump(dataset)


def set_dataset_images(client: "LocalClient", dataset: Dataset, images: Optional[List[ImageRequestModel]]):
    """Set a dataset's images.

    Args:
        client("LocalClient"): The ``LocalClient``.
        dataset(Dataset): The dataset to set images on.
        images(List[ImageRequestModel]): The images to set.

    Returns:
        True if images were set/modified.
    """
    if not images:
        images = []

    image_folder = renku_dataset_images_path(client) / dataset.initial_identifier
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
            path = os.path.normpath(os.path.join(client.path, path))

        os.remove(path)

    return images_updated or dataset.images != previous_images


def update_dataset_custom_metadata(dataset: Dataset, custom_metadata: Dict):
    """Update custom metadata on a dataset.

    Args:
        dataset(Dataset): The dataset to update.
        custom_metadata(Dict): Custom JSON-LD metadata to set.
    """

    existing_metadata = [a for a in dataset.annotations if a.source != "renku"]

    existing_metadata.append(Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku"))

    dataset.annotations = existing_metadata


@inject.autoparams("client_dispatcher", "dataset_gateway")
def move_files(
    client_dispatcher: IClientDispatcher,
    dataset_gateway: IDatasetGateway,
    files: Dict[Path, Path],
    to_dataset_name: Optional[str] = None,
):
    """Move files and their metadata from one or more datasets to a target dataset.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        dataset_gateway(IDatasetGateway):Injected dataset gateway.
        files(Dict[Path, Path]): Files to move
        to_dataset_name(Optional[str], optional): Target dataset (Default value = None)
    """
    client = client_dispatcher.current_client

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
            src = src.relative_to(client.path)
            dst = dst.relative_to(client.path)
            # NOTE: Files are moved at this point, so, we use can use dst
            new_dataset_file = DatasetFile.from_path(client, dst)

            for dataset in datasets:
                removed = dataset.unlink_file(src, missing_ok=True)
                if removed:
                    modified_datasets[dataset.name] = dataset
                    new_dataset_file.based_on = removed.based_on
                    new_dataset_file.source = removed.source
                    if not to_dataset:
                        dataset.add_or_update_files(new_dataset_file)

                # NOTE: Update dataset if it contains a destination that is being overwritten
                modified = dataset.find_file(dst)
                if modified:
                    modified_datasets[dataset.name] = dataset
                    dataset.add_or_update_files(new_dataset_file)

            if to_dataset:
                to_dataset.add_or_update_files(new_dataset_file)

            communication.update_progress(progress_name, amount=1)
    finally:
        communication.finalize_progress(progress_name)

    datasets_provenance = DatasetsProvenance()
    modified_dataset_values = list(modified_datasets.values())
    for modified_dataset in modified_dataset_values:
        datasets_provenance.add_or_update(modified_dataset, creator=get_git_user(client.repository))
    if to_dataset and to_dataset not in modified_dataset_values:
        datasets_provenance.add_or_update(to_dataset, creator=get_git_user(client.repository))


@inject.autoparams("client_dispatcher")
def update_dataset_local_files(
    client_dispatcher: IClientDispatcher, records: List[DynamicProxy]
) -> Tuple[List[DynamicProxy], List[DynamicProxy]]:
    """Update files metadata from the git history.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        records(List[DynamicProxy]): File records to update.

    Returns:
        Tuple[List[DynamicProxy], List[DynamicProxy]]: Tuple of updated and deleted file records.
    """
    client = client_dispatcher.current_client

    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []
    progress_text = "Checking for local updates"

    try:
        communication.start_progress(progress_text, len(records))
        check_paths = []
        records_to_check = []

        for file in records:
            communication.update_progress(progress_text, 1)

            if file.based_on or file.is_external:
                continue

            if not (client.path / file.entity.path).exists():
                deleted_files.append(file)
                continue

            check_paths.append(file.entity.path)
            records_to_check.append(file)

        checksums = client.repository.get_object_hashes(check_paths)

        for file in records_to_check:
            current_checksum = checksums.get(file.entity.path)
            if not current_checksum:
                deleted_files.append(file)
            elif current_checksum != file.entity.checksum:
                updated_files.append(file)
    finally:
        communication.finalize_progress(progress_text)

    return updated_files, deleted_files


@inject.autoparams()
def _update_datasets_metadata(
    new_dataset: Dataset, previous_dataset, delete, same_as, client_dispatcher: IClientDispatcher
):
    """Update metadata and remove files that exists in ``previous_dataset`` but not in ``new_dataset``.

    Args:
        new_dataset(Dataset): Dataset to update.
        previous_dataset: Dataset to update from.
        delete: Whether to delete non existing files.
        same_as: Source of the dataset.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.

    Returns:
        Dataset: Dataset with updated values.
    """
    client = client_dispatcher.current_client

    current_paths = set(str(f.entity.path) for f in new_dataset.files)

    # NOTE: remove files not present in the dataset anymore
    for file in previous_dataset.files:
        if str(file.entity.path) in current_paths:
            continue

        if delete:
            delete_file(client.path / file.entity.path, follow_symlinks=True)

    new_dataset.same_as = same_as
    # NOTE: Remove derived_from because this is an updated and imported dataset
    new_dataset.derived_from = None

    return new_dataset


def _update_datasets_files_metadata(
    client: "LocalClient",
    updated_files: List[DynamicProxy],
    deleted_files: List[DynamicProxy],
    delete: bool,
):
    modified_datasets = {}

    for file in updated_files:
        new_file = DatasetFile.from_path(
            client=client, path=file.entity.path, based_on=file.based_on, source=file.source
        )
        modified_datasets[file.dataset.name] = file.dataset
        file.dataset.add_or_update_files(new_file)

    if delete:
        for file in deleted_files:
            modified_datasets[file.dataset.name] = file.dataset
            file.dataset.unlink_file(file.entity.path)

    datasets_provenance = DatasetsProvenance()
    for dataset in modified_datasets.values():
        datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))


@inject.autoparams("client_dispatcher")
def update_dataset_git_files(
    client_dispatcher: IClientDispatcher, files: List[DynamicProxy], ref: str, delete: bool, dry_run: bool
) -> Tuple[List[DynamicProxy], List[DynamicProxy]]:
    """Update files and dataset metadata according to their remotes.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        files(List[DynamicProxy]): List of files to be updated.
        ref(str): Reference to use for update.
        delete(bool, optional): Indicates whether to delete files or not (Default value = False).
        dry_run(bool): Whether to perform update or only print changes.

    Returns:
        Tuple[List[DynamicProxy], List[DynamicProxy]]: Tuple of updated and deleted file records.
    """
    from renku.core.management.client import LocalClient

    client = client_dispatcher.current_client

    visited_repos: Dict[str, Tuple["Repository", LocalClient]] = {}
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
                remote_repository, remote_client = visited_repos[url]
            else:
                remote_repository = clone_repository(
                    url=url, path=get_cache_directory_for_repository(client=client, url=url), checkout_revision=ref
                )
                remote_client = LocalClient(path=remote_repository.path)
                visited_repos[url] = remote_repository, remote_client

            checksum = remote_repository.get_object_hash(path=based_on.path, revision="HEAD")
            found = checksum is not None
            changed = found and based_on.checksum != checksum

            src = remote_repository.path / based_on.path
            dst = client.renku_path.parent / file.entity.path

            if not found:
                if not dry_run and delete:
                    delete_file(dst, follow_symlinks=True)
                    client.repository.add(dst, force=True)
                deleted_files.append(file)
            elif changed:
                if not dry_run:
                    # Fetch file if it is tracked by Git LFS
                    remote_client.pull_paths_from_storage(remote_client.path / based_on.path)
                    if is_external_file(path=src, client_path=remote_client.path):
                        delete_file(dst, follow_symlinks=True)
                        create_external_file(client=client, target=src.resolve(), path=dst)
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


def update_external_files(client: "LocalClient", records: List[DynamicProxy], dry_run: bool) -> List[DynamicProxy]:
    """Update files linked to external storage.

    Args:
        client("LocalClient"): The ``LocalClient``.
        records(List[DynamicProxy]): File records to update.
        dry_run(bool): Whether to return a preview of what would be updated.
    """
    updated_files = []

    for file in records:
        if file.is_external:
            try:
                updated, checksum = is_external_file_updated(client_path=client.path, path=file.entity.path)
            except errors.ExternalFileNotFound as e:
                if not dry_run:
                    raise
                communication.warn(str(e))
                continue

            if updated:
                if not dry_run:
                    update_external_file(client=client, path=file.entity.path, checksum=checksum)
                updated_files.append(file)

    return updated_files


@inject.autoparams()
def filter_dataset_files(
    client_dispatcher: IClientDispatcher,
    dataset_gateway: IDatasetGateway,
    names=None,
    creators=None,
    include=None,
    exclude=None,
    ignore=None,
    immutable=False,
) -> List[DynamicProxy]:
    """Filter dataset files by specified filters.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        dataset_gateway(IDatasetGateway):Injected dataset gateway.
        names: Filter by specified dataset names. (Default value = None).
        creators: Filter by creators. (Default value = None).
        include: Include files matching file pattern. (Default value = None).
        exclude: Exclude files matching file pattern. (Default value = None).
        ignore: Ignored datasets. (Default value = None).
        immutable: Return immutable copies of dataset objects. (Default value = False).

    Returns:
        List[DynamicProxy]: List of filtered files sorted by date added.
    """
    client = client_dispatcher.current_client

    if isinstance(creators, str):
        creators = set(creators.split(","))

    if isinstance(creators, list) or isinstance(creators, tuple):
        creators = set(creators)

    records = []
    unused_names = set(names)
    for dataset in dataset_gateway.get_all_active_datasets():
        if not immutable:
            dataset = dataset.copy()
        if (not names or dataset.name in names) and (not ignore or dataset.name not in ignore):
            if unused_names:
                unused_names.remove(dataset.name)
            for file in dataset.files:
                record = DynamicProxy(file)
                record.dataset = dataset
                record.client = client
                path = Path(record.entity.path)
                match = _include_exclude(path, include, exclude)

                if creators:
                    c: Person
                    dataset_creators = {c.name for c in dataset.creators}
                    match = match and creators.issubset(dataset_creators)

                if match:
                    records.append(record)

    if unused_names:
        unused_names_str = ", ".join(unused_names)
        raise errors.ParameterError(f"Dataset does not exist: {unused_names_str}")

    return sorted(records, key=lambda r: r.date_added)


def _include_exclude(file_path, include=None, exclude=None):
    """Check if file matches one of include filters and not in exclude filter.

    Args:
        file_path: Path to the file.
        include: Tuple containing patterns to which include from result (Default value = None).
        exclude: Tuple containing patterns to which exclude from result (Default value = None).

    Returns:
        bool: True if a file should be included, False otherwise.
    """
    if exclude is not None and exclude:
        for pattern in exclude:
            if file_path.match(pattern):
                return False

    if include is not None and include:
        for pattern in include:
            if file_path.match(pattern):
                return True
        return False

    return True
