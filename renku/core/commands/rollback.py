# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku ``status`` command."""


import os.path
import re
from itertools import islice

from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.models.dataset import Dataset
from renku.core.models.provenance.activity import Activity
from renku.core.models.workflow.plan import AbstractPlan
from renku.core.utils import communication

CHECKPOINTS_PER_PAGE = 50


def rollback_command():
    """Command to perform a rollback fo the repo."""
    return Command().command(_rollback_command).require_clean().require_migration().with_database()


@inject.autoparams()
def _rollback_command(client_dispatcher: IClientDispatcher, database_dispatcher: IDatabaseDispatcher):
    """Perform a rollback of the repo."""
    current_client = client_dispatcher.current_client

    commits = current_client.repository.iterate_commits(current_client.renku_path)

    checkpoint = _prompt_for_checkpoint(commits)

    if not checkpoint:
        return

    diff = checkpoint[1].get_changes(commit="HEAD")

    confirmation_message, has_changes = _get_confirmation_message(diff, current_client)

    if not has_changes:
        communication.echo("There would be no changes rolling back to the selected command, exiting.")
        return

    communication.confirm(confirmation_message, abort=True)

    current_client.repository.reset(checkpoint[1], hard=True)


def _get_confirmation_message(diff, client):
    """Create a confirmation message for changes that would be done by a rollback."""
    modifications = _get_modifications_from_diff(client, diff)

    has_changes = False

    confirmation_message = "The following changes would be done:\n\nMetadata:\n"

    if modifications["metadata"]["restored"]:
        confirmation_message += "\nRestored ↻:\n\t" + "\n\t".join(modifications["metadata"]["restored"]) + "\n"
        has_changes = True

    if modifications["metadata"]["modified"]:
        confirmation_message += "\nModified ♻️:\n\t" + "\n\t".join(modifications["metadata"]["modified"]) + "\n"
        has_changes = True

    if modifications["metadata"]["removed"]:
        confirmation_message += "\nRemoved 🔥:\n\t" + "\n\t".join(modifications["metadata"]["removed"]) + "\n"
        has_changes = True

    confirmation_message += "\nFiles:\n"

    if modifications["files"]["restored"]:
        confirmation_message += "\nRestored ↻:\n\t" + "\n\t".join(modifications["files"]["restored"]) + "\n"
        has_changes = True

    if modifications["files"]["modified"]:
        confirmation_message += "\nModified ♻️:\n\t" + "\n\t".join(modifications["files"]["modified"]) + "\n"
        has_changes = True

    if modifications["files"]["removed"]:
        confirmation_message += "\nRemoved 🔥:\n\t" + "\n\t".join(modifications["files"]["removed"]) + "\n"
        has_changes = True

    confirmation_message += "\nProceed?"

    return confirmation_message, has_changes


def _get_modifications_from_diff(client, diff):
    """Get all modifications from a diff."""
    modifications = {
        "metadata": {"restored": [], "modified": [], "removed": []},
        "files": {"restored": [], "modified": [], "removed": []},
    }

    metadata_objects = {}

    for diff_index in diff:
        entry = diff_index.a_path or diff_index.b_path
        entry_path = client.path / entry

        if str(client.database_path) == os.path.commonpath([client.database_path, entry_path]):
            # metadata file
            entry, change_type, identifier, entry_date = _get_modification_type_from_db(entry)

            if not entry:
                continue

            if identifier not in metadata_objects or entry_date < metadata_objects[identifier][2]:
                # we only want he least recent change of a metadata object
                metadata_objects[identifier] = (entry, change_type, entry_date)

            continue

        elif str(client.renku_path) == os.path.commonpath([client.renku_path, entry_path]):
            # some other renku file
            continue

        # normal file
        if diff_index.change_type == "A":
            modifications["files"]["removed"].append(entry)

        elif diff_index.change_type == "D":
            modifications["files"]["restored"].append(entry)
        else:
            modifications["files"]["modified"].append(entry)

    for entry, change_type, _ in metadata_objects.values():
        modifications["metadata"][change_type].append(entry)

    return modifications


def _prompt_for_checkpoint(commits):
    """Ask to select a checkpoint to rollback to."""
    checkpoint_iterator = _checkpoint_iterator(commits)

    all_checkpoints = []
    current_index = 0
    selected = None

    communication.echo("Select a checkpoint to roll back to:\n")

    # prompt user to select a checkpoint in batches
    while True:
        batch = list(islice(checkpoint_iterator, CHECKPOINTS_PER_PAGE))
        more_pages = len(batch) == CHECKPOINTS_PER_PAGE
        if batch:
            all_checkpoints.extend(batch)
            prompt = "\n".join(f"[{i}] {entry[0]}" for i, entry in enumerate(batch, current_index))
            prompt += "\nCheckpoint ([q] to quit"
            default = "q"
            if more_pages:
                prompt += ", [m] for more)"
                default = "m"
            else:
                prompt += ")"

            selection = communication.prompt(prompt, default=default)
        else:
            communication.echo("No more checkpoints.")

        while True:
            # loop until user makes a valid selection
            if selection == "m" and more_pages:
                current_index += CHECKPOINTS_PER_PAGE
                break
            elif selection == "q":
                return
            else:
                try:
                    selected = int(selection)
                    if 0 <= selected < len(all_checkpoints):
                        break
                    else:
                        communication.warn("Not a valid checkpoint")
                        selected = None
                except (ValueError, TypeError):
                    communication.warn(
                        "Please enter a valid checkpoint number" + (", 'q' or 'm'" if more_pages else "or 'q")
                    )

            prompt = "Checkpoint ([q] to quit)"
            if more_pages:
                prompt += ", [m] for more)"
                default = "m"
            else:
                prompt += ")"
            selection = communication.prompt("Checkpoint ([q] to quit)", default="q")

        if selected is not None:
            break

    if not all_checkpoints:
        communication.echo("No valid renku commands in project to roll back to.")
        return

    return all_checkpoints[selected]


@inject.autoparams()
def _get_modification_type_from_db(path: str, database_dispatcher: IDatabaseDispatcher):
    """Get the modification type for an entry in the database."""
    database = database_dispatcher.current_database
    db_object = database.get(os.path.basename(path))

    if isinstance(db_object, Activity):
        return (
            f"Run: {db_object.id} (Plan name: {db_object.association.plan.name})",
            "removed",
            db_object.id,
            db_object.ended_at_time,
        )
    elif isinstance(db_object, AbstractPlan):
        change_type = "removed"

        if db_object.derived_from:
            derived = database.get_by_id(db_object.derived_from)
            if db_object.name == derived.name:
                change_type = "modified"
        if db_object.invalidated_at:
            change_type = "restored"

        return (
            f"Plan: {db_object.name}",
            change_type,
            f"plan_{db_object.name}",
            db_object.invalidated_at or db_object.date_created,
        )
    elif isinstance(db_object, Dataset):
        change_type = "removed"

        if db_object.derived_from:
            change_type = "modified"
        if db_object.date_removed:
            change_type = "restored"

        return (
            f"Dataset: {db_object.name}",
            change_type,
            f"dataset_{db_object.name}",
            db_object.date_removed or db_object.date_published or db_object.date_created,
        )
    else:
        return None, None, None, None


def _checkpoint_iterator(commits):
    """Iterate through commits to create checkpoints."""
    transaction_pattern = re.compile(r"\n\nrenku-transaction:\s([0-9a-g]+)$")

    current_checkpoint = None

    for commit in commits:
        commit_message = commit.message
        match = transaction_pattern.search(commit_message)

        if not match:
            continue

        transaction_id = match.group(0)
        entry = (
            f"{commit.authored_datetime:%Y-%m-%d %H:%M:%S} " f"\t{commit.hexsha[:7]}\t{commit_message.splitlines()[0]}",
            commit,
            transaction_id,
        )

        if not current_checkpoint:
            current_checkpoint = entry
            continue

        if transaction_id != current_checkpoint[2]:
            yield current_checkpoint
            current_checkpoint = entry

    if current_checkpoint:
        yield current_checkpoint
