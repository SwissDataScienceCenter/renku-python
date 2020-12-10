# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Repository datasets management."""
import re
import shutil
import urllib
from collections import OrderedDict
from pathlib import Path

import click
import git
import patoolib
import requests

from renku.core import errors
from renku.core.commands.format.dataset_files import DATASET_FILES_FORMATS
from renku.core.commands.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core.commands.format.datasets import DATASETS_FORMATS
from renku.core.commands.providers import ProviderFactory
from renku.core.errors import DatasetNotFound, InvalidAccessToken, OperationError, ParameterError, UsageError
from renku.core.incubation.command import Command
from renku.core.management.datasets import DATASET_METADATA_PATHS
from renku.core.models.datasets import DatasetDetailsJson, Url, generate_default_name
from renku.core.models.provenance.agents import Person
from renku.core.models.refs import LinkReference
from renku.core.models.tabulate import tabulate
from renku.core.utils import communication
from renku.core.utils.doi import is_doi
from renku.core.utils.urls import remove_credentials


def _list_datasets(client, revision=None, format=None, columns=None):
    """List all datasets."""
    if revision is None:
        datasets = client.datasets.values()
    else:
        datasets = client.datasets_from_commit(client.repo.commit(revision))

    if format is None:
        return list(datasets)

    if format not in DATASETS_FORMATS:
        raise UsageError("format not supported")

    return DATASETS_FORMATS[format](client, datasets, columns=columns)


def list_datasets():
    """Command for listing datasets."""
    return Command().command(_list_datasets).lock_dataset()


def _create_dataset(client, name, title=None, description="", creators=None, keywords=None):
    if not creators:
        creators = [Person.from_git(client.repo)]
    else:
        creators, _ = _construct_creators(creators)

    dataset, _, _ = client.create_dataset(
        name=name, title=title, description=description, creators=creators, keywords=keywords
    )

    client.update_datasets_provenance(dataset)

    return dataset


def create_dataset():
    """Return a command for creating an empty dataset in the current repo."""
    command = Command().command(_create_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _edit_dataset(client, name, title, description, creators, keywords=None):
    """Edit dataset metadata."""
    creators, no_email_warnings = _construct_creators(creators, ignore_email=True)
    title = title.strip() if isinstance(title, str) else ""

    possible_updates = {"creators": creators, "description": description, "keywords": keywords, "title": title}
    updated = [k for k, v in possible_updates.items() if v]

    if not updated:
        return [], no_email_warnings

    with client.with_dataset(name=name) as dataset:
        dataset.update_metadata(creators=creators, description=description, keywords=keywords, title=title)

    client.update_datasets_provenance(dataset)

    return updated, no_email_warnings


def edit_dataset():
    """Command for editing dataset metadata."""
    command = Command().command(_edit_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _show_dataset(client, name):
    """Show detailed dataset information."""
    dataset = client.load_dataset(name)
    return DatasetDetailsJson().dump(dataset)


def show_dataset():
    """Command for showing detailed dataset information."""
    return Command().command(_show_dataset)


def _construct_creators(creators, ignore_email=False):
    from collections.abc import Iterable

    creators = creators or ()

    if not isinstance(creators, Iterable) or isinstance(creators, str):
        raise errors.ParameterError("Invalid type")

    people = []
    no_email_warnings = []
    for creator in creators:
        if isinstance(creator, str):
            person = Person.from_string(creator)
        elif isinstance(creator, dict):
            person = Person.from_dict(creator)
        else:
            raise errors.ParameterError("Invalid type")

        message = 'A valid format is "Name <email> [affiliation]"'

        if not person.name:  # pragma: no cover
            raise errors.ParameterError(f'Name is invalid: "{creator}".\n{message}')

        if not person.email:
            if not ignore_email:  # pragma: no cover
                raise errors.ParameterError(f'Email is invalid: "{creator}".\n{message}')
            else:
                no_email_warnings.append(creator)

        people.append(person)

    return people, no_email_warnings


def _add_to_dataset(
    client,
    urls,
    name,
    external=False,
    force=False,
    overwrite=False,
    create=False,
    sources=(),
    destination="",
    ref=None,
    with_metadata=None,
    extract=False,
    all_at_once=False,
    destination_names=None,
    total_size=None,
):
    """Add data to a dataset."""
    if len(urls) == 0:
        raise UsageError("No URL is specified")
    if sources and len(urls) > 1:
        raise UsageError('Cannot use "--source" with multiple URLs.')

    if total_size is None:
        total_size = 0
        for url in urls:
            try:
                with requests.get(url, stream=True, allow_redirects=True) as r:
                    total_size += int(r.headers.get("content-length", 0))
            except requests.exceptions.RequestException:
                pass
    usage = shutil.disk_usage(client.path)

    if total_size > usage.free:
        mb = 2 ** 20
        message = "Insufficient disk space (required: {:.2f} MB" "/available: {:.2f} MB). ".format(
            total_size / mb, usage.free / mb
        )
        raise OperationError(message)

    try:
        with client.with_dataset(name=name, create=create) as dataset:
            client.add_data_to_dataset(
                dataset,
                urls=urls,
                external=external,
                force=force,
                overwrite=overwrite,
                sources=sources,
                destination=destination,
                ref=ref,
                extract=extract,
                all_at_once=all_at_once,
                destination_names=destination_names,
            )
            if with_metadata:
                # dataset has the correct list of files
                with_metadata.files = dataset.files
                with_metadata.url = dataset._id

                dataset.update_metadata_from(with_metadata)

        client.update_datasets_provenance(dataset)
    except DatasetNotFound:
        raise DatasetNotFound(
            message='Dataset "{0}" does not exist.\n'
            'Use "renku dataset create {0}" to create the dataset or retry '
            '"renku dataset add {0}" command with "--create" option for '
            "automatic dataset creation.".format(name)
        )
    except (FileNotFoundError, git.exc.NoSuchPathError) as e:
        raise ParameterError("Could not find paths/URLs: \n{0}".format("\n".join(urls))) from e


def add_to_dataset():
    """Create a command for adding data to datasets."""
    command = Command().command(_add_to_dataset).lock_dataset()
    return command.require_migration().with_commit(raise_if_empty=True, commit_only=DATASET_METADATA_PATHS)


def _list_files(client, datasets=None, creators=None, include=None, exclude=None, format=None, columns=None):
    """List dataset files."""
    records = _filter(client, names=datasets, creators=creators, include=include, exclude=exclude)
    for record in records:
        record.title = record.dataset.title
        record.dataset_name = record.dataset.name
        record.creators_csv = record.dataset.creators_csv
        record.creators_full_csv = record.dataset.creators_full_csv

    if format is None:
        return records

    if format not in DATASETS_FORMATS:
        raise UsageError("format not supported")

    return DATASET_FILES_FORMATS[format](client, records, columns=columns)


def list_files():
    """Command for listing dataset files."""
    return Command().command(_list_files).lock_dataset()


def _file_unlink(client, name, include, exclude, yes=False):
    """Remove matching files from a dataset."""
    if not include and not exclude:
        raise ParameterError(
            (
                "include or exclude filters not found.\n"
                "Check available filters with `renku dataset unlink --help`\n"
                "Hint: `renku dataset unlink my-dataset -I path`"
            )
        )

    dataset = client.load_dataset(name=name)

    if not dataset:
        raise ParameterError("Dataset does not exist.")

    records = _filter(client, names=[name], include=include, exclude=exclude)
    if not records:
        raise ParameterError("No records found.")

    if not yes:
        prompt_text = (
            f'You are about to remove following from "{name}" dataset.'
            + "\n"
            + "\n".join([str(record.full_path) for record in records])
            + "\nDo you wish to continue?"
        )
        communication.confirm(prompt_text, abort=True, warning=True)

    for item in records:
        dataset.unlink_file(item.path)

    dataset.to_yaml()
    client.update_datasets_provenance(dataset)

    return records


def file_unlink():
    """Command for removing matching files from a dataset."""
    command = Command().command(_file_unlink).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _remove_dataset(client, name):
    """Delete a dataset."""
    dataset = client.load_dataset(name=name, strict=True)
    dataset.mutate()
    dataset.to_yaml()
    client.update_datasets_provenance(dataset, remove=True)

    client.repo.git.add(dataset.path)
    client.repo.index.commit("renku dataset rm: final mutation")

    ref_path = client.get_dataset_path(name)

    metadata_path = client.path / dataset.path
    shutil.rmtree(metadata_path, ignore_errors=True)

    references = list(LinkReference.iter_items(client, common_path="datasets"))
    for ref in references:
        if ref.reference == ref_path:
            ref.delete()


def remove_dataset():
    """Command for deleting a dataset."""
    command = Command().command(_remove_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _export_dataset(
    client, name, provider_name, publish, tag, dataverse_server_url=None, dataverse_name=None,
):
    """Export data to 3rd party provider.

    :raises: ``ValueError``, ``HTTPError``, ``InvalidAccessToken``,
             ``DatasetNotFound``
    """
    provider_name = provider_name.lower()

    # TODO: all these callbacks are ugly, improve in #737
    config_key_secret = "access_token"

    dataset_ = client.load_dataset(name, strict=True)

    try:
        provider = ProviderFactory.from_id(provider_name)
    except KeyError:
        raise ParameterError("Unknown provider.")

    provider.set_parameters(client, dataverse_server_url=dataverse_server_url, dataverse_name=dataverse_name)

    selected_tag = None
    selected_commit = client.repo.head.commit

    if tag:
        selected_tag = next((t for t in dataset_.tags if t.name == tag), None)

        if not selected_tag:
            raise ValueError("Tag {} not found".format(tag))

        selected_commit = selected_tag.commit
    elif dataset_.tags and len(dataset_.tags) > 0:
        tag_result = _prompt_tag_selection(dataset_.tags)

        if tag_result:
            selected_tag = tag_result
            selected_commit = tag_result.commit

            # If the tag is created automatically for imported datasets, it
            # does not have the dataset yet and we need to use the next commit
            with client.with_commit(selected_commit):
                test_ds = client.load_dataset(name)
            if not test_ds:
                commits = client.dataset_commits(dataset_)
                next_commit = selected_commit
                for commit in commits:
                    if commit.hexsha == selected_commit:
                        selected_commit = next_commit.hexsha
                        break
                    next_commit = commit

    with client.with_commit(selected_commit):
        dataset_ = client.load_dataset(name)
        if not dataset_:
            raise DatasetNotFound(name=name)

        access_token = client.get_value(provider_name, config_key_secret)
        exporter = provider.get_exporter(dataset_, access_token=access_token)

        if access_token is None:
            access_token = _prompt_access_token(exporter)

            if access_token is None or len(access_token) == 0:
                raise InvalidAccessToken()

            client.set_value(provider_name, config_key_secret, access_token, global_only=True)
            exporter.set_access_token(access_token)

        try:
            destination = exporter.export(publish=publish, tag=selected_tag)
        except errors.AuthenticationError:
            client.remove_value(provider_name, config_key_secret, global_only=True)
            raise

    communication.echo(f"Exported to: {destination}")


def export_dataset():
    """Command for exporting a dataset to 3rd party provider."""
    command = Command().command(_export_dataset).lock_dataset()
    return command.require_migration().require_clean()


def _import_dataset(client, uri, name="", extract=False, yes=False, previous_dataset=None, delete=False):
    """Import data from a 3rd party provider or another renku project."""
    provider, err = ProviderFactory.from_uri(uri)
    if err and provider is None:
        raise ParameterError("Could not process {0}.\n{1}".format(uri, err))

    try:
        record = provider.find_record(uri, client)
        dataset = record.as_dataset(client)
        files = dataset.files
        total_size = 0

        if not yes:
            communication.echo(
                tabulate(
                    files,
                    headers=OrderedDict(
                        (("checksum", None), ("filename", "name"), ("size_in_mb", "size (mb)"), ("filetype", "type"),)
                    ),
                    floatfmt=".2f",
                )
            )

            text_prompt = "Do you wish to download this version?"
            if not record.is_last_version(uri):
                text_prompt = "Newer version found at {}\n".format(record.links.get("latest_html")) + text_prompt

            communication.confirm(text_prompt, abort=True, warning=True)

            for file_ in files:
                if file_.size_in_mb is not None:
                    total_size += file_.size_in_mb

            total_size *= 2 ** 20

    except KeyError as e:
        raise ParameterError(("Could not process {0}.\n" "Unable to fetch metadata due to {1}".format(uri, e)))

    except LookupError as e:
        raise ParameterError(("Could not process {0}.\n" "Reason: {1}".format(uri, str(e))))

    if not files:
        raise ParameterError("Dataset {} has no files.".format(uri))

    new_files = list(dataset.files)

    if not provider.is_git_based:
        if not name:
            name = generate_default_name(dataset.title, dataset.version)

        dataset.same_as = Url(url_id=remove_credentials(uri))
        if is_doi(dataset.identifier):
            dataset.same_as = Url(url_str=urllib.parse.urljoin("https://doi.org", dataset.identifier))

        urls, names = zip(*[(f.source, f.filename) for f in files])

        _add_to_dataset(
            client,
            urls=urls,
            name=name,
            create=not previous_dataset,
            with_metadata=dataset,
            force=True,
            extract=extract,
            all_at_once=True,
            destination_names=names,
            total_size=total_size,
        )

        if previous_dataset:
            dataset = _update_previous_dataset(client, dataset, previous_dataset, new_files, delete)

        if dataset.version:
            tag_name = re.sub("[^a-zA-Z0-9.-_]", "_", dataset.version)
            _tag_dataset(client, name, tag_name, "Tag {} created by renku import".format(dataset.version))
    else:
        name = name or dataset.name

        if not dataset.data_dir:
            raise OperationError(f"Data directory for dataset must be set: {dataset.name}")

        sources = [f"{dataset.data_dir}/**"]
        for file_ in dataset.files:
            try:
                Path(file_.path).relative_to(dataset.data_dir)
            except ValueError:  # Files that are not in dataset's data directory
                sources.append(file_.path)

        _add_to_dataset(
            client,
            urls=[record.project_url],
            name=name,
            sources=sources,
            with_metadata=dataset,
            create=not previous_dataset,
        )

        if previous_dataset:
            _update_previous_dataset(client, dataset, previous_dataset, new_files, delete)


def import_dataset():
    """Create a command for importing datasets."""
    command = Command().command(_import_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _update_previous_dataset(client, new_dataset, current_dataset, new_files, delete=False):
    """Update ``previous_dataset`` with changes made to ``new_dataset``."""
    current_dataset.update_metadata_from(new_dataset)
    current_files = set(f.path for f in new_files)
    # NOTE: remove files not present in the dataset anymore
    for f in current_dataset.files:
        if f.path in current_files:
            continue

        current_dataset.unlink_file(f.path)

        if delete:
            client.remove_file(client.path / f.path)

    current_dataset.to_yaml()
    return current_dataset


def _update_datasets(
    client, names, creators, include, exclude, ref, delete, external=False,
):
    """Update files from a remote Git repo."""
    ignored_datasets = []

    if (include or exclude) and names and any(d.same_as for d in client.datasets.values() if d.name in names):
        raise errors.UsageError(
            "--include/--exclude is incompatible with datasets created by" " `renku dataset import`"
        )

    names_provided = bool(names)

    # NOTE: update imported datasets
    if not include and not exclude:
        for dataset in client.datasets.values():
            if names and dataset.name not in names or not dataset.same_as:
                continue

            uri = dataset.same_as.url
            provider, err = ProviderFactory.from_uri(uri)

            if not provider:
                continue

            record = provider.find_record(uri, client)

            if record.is_last_version(uri) and record.version == dataset.version:
                continue

            uri = record.latest_uri

            # NOTE: set extract to false if there are any archives present in the dataset
            extract = True
            for f in dataset.files:
                try:
                    patoolib.get_archive_format(f.path)
                except patoolib.util.PatoolError:
                    continue
                else:
                    extract = False
                    break

            _import_dataset(
                client, uri, name=dataset.name, extract=extract, yes=True, previous_dataset=dataset, delete=delete
            )

            communication.echo(f"Updated dataset {dataset.name} from remote provider")

            if names:
                names.remove(dataset.name)
            ignored_datasets.append(dataset.name)
    else:
        ignored_datasets = [d.name for d in client.datasets.values() if d.same_as]

    if names_provided and not names:
        return
    records = _filter(
        client, names=names, creators=creators, include=include, exclude=exclude, ignore=ignored_datasets,
    )

    if not records:
        raise ParameterError("No files matched the criteria.")

    possible_updates = []
    unique_remotes = set()
    external_files = []

    for file_ in records:
        if file_.based_on:
            possible_updates.append(file_)
            unique_remotes.add(file_.based_on.source)
        elif file_.external:
            external_files.append(file_)

    if ref and len(unique_remotes) > 1:
        raise ParameterError(
            'Cannot use "--ref" with more than one Git repository.\n'
            "Limit list of files to be updated to one repository. See"
            '"renku dataset update -h" for more information.'
        )

    if external_files:
        if external:
            client.update_external_files(external_files)
        else:
            communication.echo("To update external files run update command with '--external' flag.")

    if not possible_updates:
        return

    updated_files, deleted_files = client.update_dataset_git_files(files=possible_updates, ref=ref, delete=delete)

    if deleted_files and not delete:
        communication.echo(
            "Some files are deleted from remote. To also delete them locally "
            "run update command with `--delete` flag."
        )
    communication.echo("Updated {} files".format(len(updated_files)))


def update_datasets():
    """Command for updating datasets."""
    command = Command().command(_update_datasets).lock_dataset()
    return command.require_migration().require_clean().with_commit(commit_only=DATASET_METADATA_PATHS)


def _include_exclude(file_path, include=None, exclude=None):
    """Check if file matches one of include filters and not in exclude filter.

    :param file_path: Path to the file.
    :param include: Tuple containing patterns to which include from result.
    :param exclude: Tuple containing patterns to which exclude from result.
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


def _filter(client, names=None, creators=None, include=None, exclude=None, ignore=None):
    """Filter dataset files by specified filters.

    :param names: Filter by specified dataset names.
    :param creators: Filter by creators.
    :param include: Include files matching file pattern.
    :param exclude: Exclude files matching file pattern.
    :param ignore: Ignored datasets.
    """
    if isinstance(creators, str):
        creators = set(creators.split(","))

    if isinstance(creators, list) or isinstance(creators, tuple):
        creators = set(creators)

    records = []
    unused_names = set(names)
    for dataset in client.datasets.values():
        if (not names or dataset.name in names) and (not ignore or dataset.name not in ignore):
            if unused_names:
                unused_names.remove(dataset.name)
            for file_ in dataset.files:
                file_.dataset = dataset
                file_.client = client
                path = Path(file_.path)
                match = _include_exclude(path, include, exclude)

                if creators:
                    dataset_creators = {c.name for c in dataset.creators}
                    match = match and creators.issubset(dataset_creators)

                if match:
                    records.append(file_)

    if unused_names:
        unused_names = ", ".join(unused_names)
        raise ParameterError(f"Dataset does not exist: {unused_names}")

    return sorted(records, key=lambda r: r.added)


def _tag_dataset(client, name, tag, description, force=False):
    """Creates a new tag for a dataset."""
    dataset = client.load_dataset(name, strict=True)

    try:
        client.add_dataset_tag(dataset, tag, description, force)
    except ValueError as e:
        raise ParameterError(e)
    else:
        dataset.to_yaml()
        client.update_datasets_provenance(dataset)


def tag_dataset():
    """Command for creating a new tag for a dataset."""
    command = Command().command(_tag_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _remove_dataset_tags(client, name, tags):
    """Removes tags from a dataset."""
    dataset = client.load_dataset(name, strict=True)

    try:
        client.remove_dataset_tags(dataset, tags)
    except ValueError as e:
        raise ParameterError(e)
    else:
        dataset.to_yaml()
        client.update_datasets_provenance(dataset)


def remove_dataset_tags():
    """Command for removing tags from a dataset."""
    command = Command().command(_remove_dataset_tags).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _list_tags(client, name, format):
    """List all tags for a dataset."""
    dataset = client.load_dataset(name, strict=True)

    tags = sorted(dataset.tags, key=lambda t: t.created)

    return DATASET_TAGS_FORMATS[format](client, tags)


def list_tags():
    """Command for listing a dataset's tags."""
    return Command().command(_list_tags).lock_dataset()


def _prompt_access_token(exporter):
    """Prompt user for an access token for a provider.

    :return: The new access token
    """
    text_prompt = "You must configure an access token\n"
    text_prompt += "Create one at: {0}\n".format(exporter.access_token_url())
    text_prompt += "Access token"

    return communication.prompt(text_prompt, type=str)


def _prompt_tag_selection(tags):
    """Prompt user to chose a tag or <HEAD>."""
    # Prompt user to select a tag to export
    tags = sorted(tags, key=lambda t: t.created)

    text_prompt = "Tag to export: \n\n<HEAD>\t[1]\n"

    text_prompt += "\n".join("{}\t[{}]".format(t.name, i) for i, t in enumerate(tags, start=2))

    text_prompt += "\n\nTag"
    selection = communication.prompt(text_prompt, type=click.IntRange(1, len(tags) + 1), default=1)

    if selection > 1:
        return tags[selection - 2]
    return None
