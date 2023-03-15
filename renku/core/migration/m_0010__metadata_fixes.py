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
"""Various metadata migrations for v10."""

import io
import json
import os
from datetime import timezone
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

import zstandard as zstd

from renku.command.checks.activities import fix_activity_dates
from renku.command.checks.workflow import fix_plan_dates
from renku.command.command_builder import inject
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.migration.models import v10 as old_schema
from renku.core.migration.models.migration import MigrationContext, MigrationType
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.domain_model.dataset import Dataset
from renku.domain_model.project import Project, ProjectTemplateMetadata
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.activity import Activity
from renku.domain_model.workflow.plan import AbstractPlan


def migrate(migration_context: MigrationContext):
    """Migration function."""
    from renku.infrastructure.gateway.activity_gateway import reindex_catalog

    migrate_project_template_data()
    migrate_old_metadata_namespaces()

    if MigrationType.WORKFLOWS in migration_context.options.type:
        migrate_activity_ids()
        fix_plan_times()
        fix_activity_times()

    migrate_remote_entity_ids()
    fix_dataset_date_modified()
    fix_dataset_image_ids()
    fix_removed_plans()

    # NOTE: Rebuild all workflow catalogs since ids and times have changed
    communication.echo("Rebuilding workflow metadata")
    reindex_catalog(project_context.database)


def migrate_old_metadata_namespaces():
    """Migrate old namespaces (e.g. renku.cli.* -> renku.ui.cli.*)."""
    communication.echo("Migrating old namespaces")
    compressor = zstd.ZstdCompressor()
    decompressor = zstd.ZstdDecompressor()
    for root, _, files in os.walk(project_context.database_path):
        root = Path(root)
        for file_name in files:
            path = root / file_name
            compressed = False
            with open(path, "rb") as file:
                header = int.from_bytes(file.read(4), "little")
                file.seek(0)
                if header == zstd.MAGIC_NUMBER:
                    with decompressor.stream_reader(file) as compressed_file:
                        data = json.load(compressed_file)
                        compressed = True
                else:
                    data = json.load(file)

            # modify namespace
            transformation = [
                ("renku.core.models.", "renku.domain_model."),
                ("renku.core.metadata.", "renku.infrastructure."),
            ]
            nested_update(data, target_key="@renku_data_type", transforms=transformation)

            if compressed:
                with open(path, "wb") as fb, compressor.stream_writer(fb) as compression_writer:
                    with io.TextIOWrapper(compression_writer) as out:
                        json.dump(data, out, ensure_ascii=False)
            else:
                with open(path, "w") as ft:
                    json.dump(data, ft, ensure_ascii=False, sort_keys=True, indent=2)


def nested_update(data: Dict[str, Any], target_key: str, transforms: List[Tuple[str, str]]) -> None:
    """Update a key's value based on transformations (from, to) in a deeply nested dictionary."""
    for k in list(data.keys()):
        value = data[k]
        if isinstance(value, str) and k == target_key:
            for lookup, replacement in transforms:
                value = value.replace(lookup, replacement)
            data[k] = value
        elif isinstance(value, dict):
            nested_update(value, target_key=target_key, transforms=transforms)
        elif isinstance(value, list) and len(value) > 0 and any(isinstance(v, dict) for v in value):
            result = []
            for v in value:
                if isinstance(v, dict):
                    nested_update(v, target_key=target_key, transforms=transforms)

                result.append(v)
            data[k] = result


def migrate_activity_ids():
    """Fix activity related ids.

    Fixes ids not starting with `/activities/`.
    Fixes generations having `/usages/` in their id.
    """
    communication.echo("Fixing activity ids")

    database = project_context.database

    activities: List[Activity] = list(database["activities"].values())

    def fix_id(id: str, changed: bool) -> Tuple[str, bool]:
        """Fix an activity related id."""
        if not id.startswith("/activities/"):
            return f"/activities/{id.lstrip('/')}", True
        return id, changed

    for activity in activities:
        changed = False
        activity.unfreeze()
        old_activity_id = activity.id
        activity.id, changed = fix_id(activity.id, changed)
        activity.association.id, changed = fix_id(activity.association.id, changed)

        for generation in activity.generations:
            id, changed = fix_id(generation.id, changed)

            if "/usages/" in generation.id:
                id = generation.id.replace("/usages/", "/generations/")
                changed = True
            object.__setattr__(generation, "id", id)

        for entity in chain(activity.usages, activity.parameters):
            id, changed = fix_id(entity.id, changed)
            object.__setattr__(entity, "id", id)

        if changed:
            database["activities"].pop(old_activity_id)
            activity._p_changed = True
            old_id = str(activity._p_oid)
            os.unlink(project_context.database_path / old_id[0:2] / old_id[2:4] / old_id)
            activity.reassign_oid()
            database["activities"].add(activity)
        activity.freeze()
    database["activities"]._p_changed = True
    database.commit()


def migrate_remote_entity_ids():
    """Change `remote-entity` to `remote-entities` in ids."""
    database = project_context.database

    def fix_dataset_files_based_on(dataset):
        changed = False
        for file in dataset.dataset_files:
            if file.based_on is not None:
                file.based_on.id = file.based_on.id.replace("/remote-entity//", "/remote-entities/")
                file.based_on.id = file.based_on.id.replace("/remote-entity/", "/remote-entities/")
                changed = True

        if changed:
            dataset._p_changed = True

    datasets: List[Dataset] = list(database["datasets-provenance-tails"].values())

    for dataset in datasets:
        fix_dataset_files_based_on(dataset)

        while dataset.derived_from is not None:
            dataset = database.get_by_id(id=dataset.derived_from.url_id)
            fix_dataset_files_based_on(dataset)

    database.commit()


@inject.autoparams()
def migrate_project_template_data(project_gateway: IProjectGateway):
    """Migrate project template metadata to nested model."""
    if not hasattr(project_context.project, "template_source"):
        # NOTE: already on newest version
        return

    old_project = cast(
        old_schema.Project,
        project_context.database.get_from_path(
            "project", override_type=f"{old_schema.Project.__module__}.{old_schema.Project.__qualname__}"
        ),
    )

    project = Project(
        agent_version=old_project.agent_version,
        annotations=old_project.annotations,
        creator=old_project.creator,
        date_created=old_project.date_created,
        description=old_project.description,
        id=old_project.id,
        name=old_project.name,
        version=old_project.version,
        keywords=old_project.keywords,
        template_metadata=ProjectTemplateMetadata(
            template_id=old_project.template_id,
            metadata=old_project.template_metadata,
            template_ref=old_project.template_ref,
            template_source=old_project.template_source,
            template_version=old_project.template_version,
            immutable_template_files=old_project.immutable_template_files,
        ),
    )

    project_gateway.update_project(project)
    project_context.database.commit()


@inject.autoparams("plan_gateway")
def fix_plan_times(plan_gateway: IPlanGateway):
    """Add timezone to plan invalidations."""
    plans: List[AbstractPlan] = plan_gateway.get_all_plans()

    for plan in plans:
        plan.unfreeze()
        if hasattr(plan, "invalidated_at"):
            plan.date_removed = plan.invalidated_at
            del plan.invalidated_at
        elif not hasattr(plan, "date_removed"):
            plan.date_removed = None

        if plan.date_removed is not None:
            if plan.date_removed.tzinfo is None:
                # NOTE: There was a bug that caused date_removed to be set without timezone (as UTC time)
                # so we patch in the timezone here
                plan.date_removed = plan.date_removed.astimezone(timezone.utc)
            if plan.date_removed < plan.date_created:
                # NOTE: Fix invalidation times set before creation date on plans
                plan.date_removed = plan.date_created
        plan.freeze()

    fix_plan_dates(plans=plans, plan_gateway=plan_gateway)
    project_context.database.commit()


@inject.autoparams("activity_gateway")
def fix_activity_times(activity_gateway: IActivityGateway):
    """Make sure activities have valid start/end/delete dates."""
    fix_activity_dates(activities=activity_gateway.get_all_activities(include_deleted=True))
    project_context.database.commit()


@inject.autoparams("dataset_gateway")
def fix_dataset_date_modified(dataset_gateway: IDatasetGateway):
    """Change date_created and date_modified to have correct semantics."""

    def fix_creation_date(dataset):
        """Check creation date to make sure that it's after project's creation date."""
        if dataset.date_created and dataset.date_created < project_context.project.date_created:
            dataset.unfreeze()
            try:
                dataset.date_created = min([f.date_added for f in dataset.files])
            except (ValueError, TypeError):
                dataset.date_created = project_context.project.date_created
            else:
                if dataset.date_created < project_context.project.date_created:
                    dataset.date_created = project_context.project.date_created
            dataset.freeze()

    tails = dataset_gateway.get_provenance_tails()

    for dataset_tail in tails:
        dataset = dataset_tail
        found_datasets: List[Dataset] = []
        previous_modification_date = local_now()

        while dataset.derived_from is not None:
            fix_creation_date(dataset)
            modification_date = dataset.date_removed or dataset.date_created

            if modification_date is not None:
                # NOTE: This happened in a project due to a timezone change
                if modification_date > previous_modification_date:
                    modification_date = previous_modification_date
                dataset.unfreeze()
                dataset.date_modified = modification_date
                dataset.freeze()
                previous_modification_date = modification_date
            found_datasets.append(dataset)
            dataset = dataset_gateway.get_by_id(dataset.derived_from.value)

        fix_creation_date(dataset)
        # NOTE: first dataset in chain
        modification_date = dataset.date_published or dataset.date_created
        if modification_date is not None:
            dataset.unfreeze()
            dataset.date_modified = modification_date
            dataset.freeze()

        for child in found_datasets:
            child.unfreeze()
            child.date_created = dataset.date_created
            child.date_published = dataset.date_published
            child.freeze()

    project_context.database.commit()


@inject.autoparams("dataset_gateway")
def fix_dataset_image_ids(dataset_gateway: IDatasetGateway):
    """Remove dashes from dataset image IDs."""
    for dataset in dataset_gateway.get_provenance_tails():
        while True:
            if dataset.images:
                for image in dataset.images:
                    image.id = image.id.replace("-", "")

                dataset._p_changed = True

            if not dataset.derived_from:
                break

            dataset = dataset_gateway.get_by_id(dataset.derived_from.value)

    project_context.database.commit()


@inject.autoparams("plan_gateway")
def fix_removed_plans(plan_gateway: IPlanGateway):
    """Create a derivative if a removed plan doesn't have one."""
    plans: List[AbstractPlan] = plan_gateway.get_all_plans()

    for plan in plans:
        if plan.date_removed and plan.derived_from is None:
            derived_plan = plan.derive()
            derived_plan.date_modified = plan.date_modified
            derived_plan.delete(when=plan.date_removed)
            plan_gateway.add(derived_plan)

            plan.unfreeze()
            plan.date_removed = None
            plan.freeze()

    project_context.database.commit()
