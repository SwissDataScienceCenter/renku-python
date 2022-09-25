# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Checks needed to determine integrity of datasets."""

import os
from collections import defaultdict

import click

from renku.command.command_builder import inject
from renku.command.util import WARNING
from renku.core import errors
from renku.core.dataset.dataset_add import add_to_dataset
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.migration.utils import get_pre_0_3_4_datasets_metadata
from renku.core.util import communication
from renku.core.util.os import get_safe_relative_path
from renku.domain_model.project_context import project_context


def check_dataset_old_metadata_location(**_):
    """Check location of dataset metadata.

    Args:
        _: keyword arguments.

    Returns:
        Tuple of whether dataset metadata location is valid and string of found problems.
    """
    old_metadata = get_pre_0_3_4_datasets_metadata()

    if not old_metadata:
        return True, None

    problems = (
        WARNING + "There are metadata files in the old location."
        '\n  (use "renku migrate" to move them)\n\n\t'
        + "\n\t".join(click.style(str(path.relative_to(project_context.path)), fg="yellow") for path in old_metadata)
        + "\n"
    )

    return False, problems


@inject.autoparams("dataset_gateway")
def check_missing_files(dataset_gateway: IDatasetGateway, **_):
    """Find missing files listed in datasets.

    Args:
        dataset_gateway(IDatasetGateway): the dataset gateway.
        _: keyword arguments.

    Returns:
        Tuple of whether all dataset files are there and string of found problems.
    """
    missing = defaultdict(list)

    for dataset in dataset_gateway.get_all_active_datasets():
        for file_ in dataset.files:
            path = project_context.path / file_.entity.path
            file_exists = path.exists() or (file_.is_external and os.path.lexists(path))
            if not file_exists:
                missing[dataset.name].append(file_.entity.path)

    if not missing:
        return True, None

    problems = WARNING + "There are missing files in datasets."

    for dataset_name, files in missing.items():
        problems += (
            "\n\t"
            + click.style(dataset_name, fg="yellow")
            + ":\n\t  "
            + "\n\t  ".join(click.style(path, fg="red") for path in files)
        )

    return False, problems


@inject.autoparams("dataset_gateway")
def check_invalid_datasets_derivation(fix, dataset_gateway: IDatasetGateway, **_):
    """Remove ``derived_from`` from import datasets.

    Args:
        fix: Whether to fix found issues.
        dataset_gateway(IDatasetGateway): the dataset gateway.
        _: keyword arguments.

    Returns:
        Tuple of whether dataset derivations are valid and string of found problems.
    """
    invalid_datasets = []

    def fix_or_report(dataset):
        if fix:
            dataset.unfreeze()
            dataset.derived_from = None
            dataset.freeze()
            communication.info(f"Fixing dataset '{dataset.name}'")
        else:
            invalid_datasets.append(dataset.name)

    for dataset in dataset_gateway.get_provenance_tails():
        while dataset.derived_from is not None and dataset.derived_from.url_id is not None:
            if dataset.same_as or dataset.derived_from.url_id == dataset.id:
                fix_or_report(dataset)
                break

            try:
                dataset = dataset_gateway.get_by_id(dataset.derived_from.url_id)
            except errors.ObjectNotFoundError:
                fix_or_report(dataset)
                break

    if not invalid_datasets:
        return True, None

    problems = (
        WARNING
        + "There are invalid dataset metadata in the project (use 'renku doctor --fix' to fix them):"
        + "\n\n\t"
        + "\n\t".join(click.style(name, fg="yellow") for name in invalid_datasets)
        + "\n"
    )

    return False, problems


@inject.autoparams("dataset_gateway")
def check_dataset_files_outside_datadir(fix, dataset_gateway: IDatasetGateway, **_):
    """Check for dataset files that are not inside a dataset's datadir.

    Args:
        fix: Whether to fix found issues.
        dataset_gateway(IDatasetGateway): the dataset gateway.
        _: keyword arguments.

    Returns:
        Tuple of whether there are no dataset files outside of its datadir and string of found problems.
    """
    invalid_files = []
    for dataset in dataset_gateway.get_provenance_tails():
        if dataset.date_removed:
            continue

        data_dir = dataset.get_datadir()

        detected_files = []

        for file in dataset.files:
            if file.is_external:
                continue
            try:
                get_safe_relative_path(project_context.path / file.entity.path, project_context.path / data_dir)
            except ValueError:
                detected_files.append(file)

        if not detected_files:
            continue

        if fix:
            communication.info(f"Fixing dataset '{dataset.name}' files.")
            dataset.unfreeze()
            for file in detected_files:
                dataset.unlink_file(file.entity.path)
            dataset.freeze()
            add_to_dataset(dataset.name, urls=[file.entity.path for file in detected_files], link=True)
        else:
            invalid_files.extend(detected_files)

    if invalid_files:
        problems = (
            WARNING
            + "There are dataset files that aren't inside their dataset's data directory "
            + "(use 'renku doctor --fix' to fix them):\n\n\t"
            + "\n\t".join(click.style(file.entity.path, fg="yellow") for file in invalid_files)
            + "\n"
        )
        return False, problems

    return True, None
