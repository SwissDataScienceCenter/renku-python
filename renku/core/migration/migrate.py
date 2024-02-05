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
"""Renku migrations management.

Migrations files are put in renku/core/management/migrations directory. Name
of these files has ``m_1234__name.py`` format where 1234 is the migration version
and name can be any alphanumeric and underscore combination. Migration files
are sorted based on their lowercase name. Each migration file must define a
public ``migrate`` function that accepts a ``MigrationContext`` as its argument.

When executing a migration, the migration file is imported as a module and the
``migrate`` function is executed. Renku checks project's metadata version and
applies any migration that has a higher version to the project.
"""

import importlib
import re
import shutil
from pathlib import Path
from typing import Optional, Tuple

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
from renku.core.migration.utils import is_using_temporary_datasets_path, read_project_version
from renku.core.template.usecase import calculate_dockerfile_checksum, update_dockerfile_checksum
from renku.core.util import communication
from renku.core.util.metadata import (
    is_renku_project,
    read_renku_version_from_dockerfile,
    replace_renku_version_in_dockerfile,
)
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


def is_docker_update_possible() -> bool:
    """Check if the Dockerfile can be updated to a new version of renku-python."""
    return update_dockerfile(check_only=True)[0]


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

    n_migrations_executed = 0

    if not skip_migrations:
        project_version = project_version or get_project_version()

        migration_context = MigrationContext(
            strict=strict, type=migration_type, preserve_identifiers=preserve_identifiers
        )

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

        # we might not have been able to tell if a docker update is possible due to outstanding migrations.
        # so we need to check again here.
        skip_docker_update |= not is_docker_update_possible()

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
        except Exception as e:
            raise TemplateUpdateError("Couldn't update from template.") from e

    if (
        not skip_docker_update
        and project
        and hasattr(project, "template_metadata")
        and isinstance(project.template_metadata, ProjectTemplateMetadata)
    ):
        try:
            docker_updated, _, _ = update_dockerfile()
        except DockerfileUpdateError:
            raise
        except Exception as e:
            raise DockerfileUpdateError("Couldn't update renku version in Dockerfile.") from e

    return n_migrations_executed != 0, template_updated, docker_updated


def _remove_untracked_renku_files(metadata_path):
    from renku.core.constant import CACHE

    untracked_paths = [RENKU_TMP, CACHE, "vendors"]
    for path in untracked_paths:
        path = metadata_path / path
        shutil.rmtree(path, ignore_errors=True)


def _update_template() -> bool:
    """Update local files from the remote template."""
    from renku.core.template.usecase import update_template

    try:
        project = project_context.project
    except ValueError:
        # NOTE: Old project, we don't know the status until it is migrated
        return False

    if not hasattr(project, "template_metadata") or not project.template_metadata.template_version:
        return False

    return bool(update_template(interactive=False, force=False, dry_run=False))


def update_dockerfile(*, check_only=False) -> Tuple[bool, Optional[bool], Optional[str]]:
    """Update the dockerfile to the newest version of renku."""
    from renku import __version__

    if not project_context.dockerfile_path.exists():
        return False, None, None

    with open(project_context.dockerfile_path) as f:
        dockerfile_content = f.read()

    docker_version = read_renku_version_from_dockerfile()
    if not docker_version:
        if check_only:
            return False, None, None
        raise DockerfileUpdateError(
            "Couldn't update renku-python version in Dockerfile, as it doesn't contain an 'ARG RENKU_VERSION=...' line."
        )

    current_version = Version(Version(__version__).base_version)
    if Version(docker_version.base_version) >= current_version:
        return True, False, str(docker_version)

    if check_only:
        return True, True, str(docker_version)

    communication.echo("Updating dockerfile...")

    new_content = replace_renku_version_in_dockerfile(dockerfile_content=dockerfile_content, version=__version__)
    new_checksum = calculate_dockerfile_checksum(dockerfile_content=new_content)

    try:
        update_dockerfile_checksum(new_checksum=new_checksum)
    except DockerfileUpdateError:
        pass

    with open(project_context.dockerfile_path, "w") as f:
        f.write(new_content)

    communication.echo("Updated dockerfile.")

    return True, False, str(current_version)


def get_project_version():
    """Get the metadata version the renku project is on."""
    try:
        return int(read_project_version())
    except ValueError:
        return 1


def get_migrations():
    """Return a sorted list of versions and migration modules."""
    migrations = []
    for entry in importlib_resources.files("renku.core.migration").iterdir():
        match = re.search(r"^m_([0-9]{4})__[a-zA-Z0-9_-]*.py$", entry.name)

        if match is None:  # migration files match m_0000__[name].py format
            continue

        version = int(match.groups()[0])
        path = f"renku.core.migration.{Path(entry.name).stem}"
        migrations.append((version, path))

    migrations = sorted(migrations, key=lambda v: v[1].lower())
    return migrations
