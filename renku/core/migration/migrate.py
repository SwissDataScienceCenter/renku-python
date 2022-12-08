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
"""Renku migrations management.

Migrations files are put in renku/core/management/migrations directory. Name
of these files has ``m_1234__name.py`` format where 1234 is the migration version
and name can be any alphanumeric and underscore combination. Migration files
are sorted based on their lowercase name. Each migration file must define a
public ``migrate`` function that accepts a ``MigrationContext`` as its argument.

When executing a migration, the migration file is imported as a module and the
``migrate`` function is executed. Migration version is checked against the Renku
project version and any migration which has a higher version is applied to the
project.
"""

import importlib
import re
import shutil
from pathlib import Path

from packaging.version import Version

from renku.command.command_builder.command import inject
from renku.core.constant import RENKU_TMP
from renku.core.errors import (
    DockerfileUpdateError,
    MigrationError,
    MigrationRequired,
    ProjectNotSupported,
    TemplateUpdateError,
)
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.migration.models.migration import MigrationContext, MigrationType
from renku.core.migration.utils import OLD_METADATA_PATH, is_using_temporary_datasets_path, read_project_version
from renku.core.util import communication
from renku.domain_model.project import ProjectTemplateMetadata
from renku.domain_model.project_context import project_context

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources  # type: ignore

SUPPORTED_PROJECT_VERSION = 10


def check_for_migration():
    """Checks if migration is required."""
    if is_migration_required():
        raise MigrationRequired
    elif is_project_unsupported():
        raise ProjectNotSupported


def is_migration_required():
    """Check if project requires migration."""
    return is_renku_project() and get_project_version() < SUPPORTED_PROJECT_VERSION


def is_project_unsupported():
    """Check if this version of Renku cannot work with the project."""
    return is_renku_project() and get_project_version() > SUPPORTED_PROJECT_VERSION


def is_docker_update_possible():
    """Check if the Dockerfile can be updated to a new version of renku-python."""
    return _update_dockerfile(check_only=True)


@inject.autoparams("project_gateway")
def migrate_project(
    project_gateway: IProjectGateway,
    force_template_update=False,
    skip_template_update=False,
    skip_docker_update=False,
    skip_migrations=False,
    project_version=None,
    max_version=None,
    strict=False,
    migration_type: MigrationType = MigrationType.ALL,
    preserve_identifiers=False,
):
    """Migrate all project's entities.

    NOTE: The project path must be pushed to the project_context before calling this function.

    Args:
        project_gateway(IProjectGateway): The injected project gateway.
        force_template_update: Whether to force update the template  (Default value = False).
        skip_template_update: Whether to skip updating the template (Default value = False).
        skip_docker_update: Whether to skip updating the Dockerfile (Default value = False).
        skip_migrations: Whether to skip migrating project metadata (Default value = False).
        project_version: Starting migration version (Default value = False).
        max_version: Apply migration up to the given version (Default value = False).
        strict: Whether to fail on errors (Default value = False).
        migration_type(MigrationType): Type of migration to perform (Default value = MigrationType.ALL).
        preserve_identifiers: Whether to preserve ids when migrating metadata (Default value = False).

    Returns:
        Dictionary of project migration status.
    """
    template_updated = docker_updated = False
    if not is_renku_project():
        return False, template_updated, docker_updated

    try:
        project = project_context.project
    except ValueError:
        project = None

    if (
        not skip_template_update
        and project
        and hasattr(project, "template_metadata")
        and isinstance(project.template_metadata, ProjectTemplateMetadata)
        and project.template_metadata.template_source
    ):
        try:
            template_updated = _update_template()
        except TemplateUpdateError:
            raise
        except (Exception, BaseException) as e:
            raise TemplateUpdateError("Couldn't update from template.") from e

    if not skip_docker_update:
        try:
            docker_updated, _, _ = _update_dockerfile()
        except DockerfileUpdateError:
            raise
        except (Exception, BaseException) as e:
            raise DockerfileUpdateError("Couldn't update renku version in Dockerfile.") from e

    if skip_migrations:
        return False, template_updated, docker_updated

    project_version = project_version or get_project_version()
    n_migrations_executed = 0

    migration_context = MigrationContext(strict=strict, type=migration_type, preserve_identifiers=preserve_identifiers)

    version = 1
    for version, path in get_migrations():
        if max_version and version > max_version:
            break
        if version > project_version:
            module = importlib.import_module(path)
            module_name = module.__name__.split(".")[-1]
            communication.echo(f"Applying migration {module_name}...")
            try:
                module.migrate(migration_context)
            except (Exception, BaseException) as e:
                raise MigrationError("Couldn't execute migration") from e
            n_migrations_executed += 1
    if not is_using_temporary_datasets_path():
        if n_migrations_executed > 0:
            project_context.project.version = str(version)
            project_gateway.update_project(project_context.project)

            communication.echo(f"Successfully applied {n_migrations_executed} migrations.")

        _remove_untracked_renku_files(metadata_path=project_context.metadata_path)

    return n_migrations_executed != 0, template_updated, docker_updated


def _remove_untracked_renku_files(metadata_path):
    from renku.core.constant import CACHE

    untracked_paths = [RENKU_TMP, CACHE, "vendors"]
    for path in untracked_paths:
        path = metadata_path / path
        shutil.rmtree(path, ignore_errors=True)


@inject.autoparams()
def _update_template(project_gateway: IProjectGateway) -> bool:
    """Update local files from the remote template."""
    from renku.core.template.usecase import update_template

    try:
        project = project_gateway.get_project()
    except ValueError:
        # NOTE: Old project, we don't know the status until it is migrated
        return False

    if not project.template_version:
        return False

    return bool(update_template(interactive=False, force=False, dry_run=False))


def _update_dockerfile(check_only=False):
    """Update the dockerfile to the newest version of renku."""
    from renku import __version__

    if not project_context.docker_path.exists():
        return False, None, None

    communication.echo("Updating dockerfile...")

    with open(project_context.docker_path, "r") as f:
        dockerfile_content = f.read()

    current_version = Version(__version__)
    m = re.search(r"^ARG RENKU_VERSION=(\d+\.\d+\.\d+)$", dockerfile_content, flags=re.MULTILINE)
    if not m:
        if check_only:
            return False, None, None
        raise DockerfileUpdateError(
            "Couldn't update renku-python version in Dockerfile, as it doesn't contain an 'ARG RENKU_VERSION=...' line."
        )

    docker_version = Version(m.group(1))

    if docker_version >= current_version:
        return True, False, str(docker_version)

    if check_only:
        return True, True, str(docker_version)

    dockerfile_content = re.sub(
        r"^ARG RENKU_VERSION=\d+\.\d+\.\d+$", f"ARG RENKU_VERSION={__version__}", dockerfile_content, flags=re.MULTILINE
    )

    with open(project_context.docker_path, "w") as f:
        f.write(dockerfile_content)

    communication.echo("Updated dockerfile.")

    return True, False, str(current_version)


def get_project_version():
    """Get the metadata version the renku project is on."""
    try:
        return int(read_project_version())
    except ValueError:
        return 1


def is_renku_project() -> bool:
    """Check if repository is a renku project."""
    try:
        return project_context.project is not None
    except ValueError:  # NOTE: Error in loading due to an older schema
        return project_context.metadata_path.joinpath(OLD_METADATA_PATH).exists()


def get_migrations():
    """Return a sorted list of versions and migration modules."""
    migrations = []
    for entry in importlib_resources.files("renku.core.migration").iterdir():
        match = re.search(r"^m_([0-9]{4})__[a-zA-Z0-9_-]*.py$", entry.name)

        if match is None:  # migration files match m_0000__[name].py format
            continue

        version = int(match.groups()[0])
        path = "renku.core.migration.{}".format(Path(entry.name).stem)
        migrations.append((version, path))

    migrations = sorted(migrations, key=lambda v: v[1].lower())
    return migrations
