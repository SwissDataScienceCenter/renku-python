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
"""Migrate project to the latest Renku version."""

from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from pydantic import ConfigDict, validate_call

from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.errors import MinimumVersionError
from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION
from renku.domain_model.project_context import project_context

SUPPORTED_RENKU_PROJECT = 1
MIGRATION_REQUIRED = 2
UNSUPPORTED_PROJECT = 4
NON_RENKU_REPOSITORY = 8
TEMPLATE_UPDATE_POSSIBLE = 16
AUTOMATED_TEMPLATE_UPDATE_SUPPORTED = 32
DOCKERFILE_UPDATE_POSSIBLE = 64


@dataclass
class CoreStatusResult:
    """Core migration status."""

    migration_required: bool
    project_metadata_version: Optional[int]
    current_metadata_version: int


@dataclass
class DockerfileStatusResult:
    """Docker migration status."""

    automated_dockerfile_update: bool
    newer_renku_available: Optional[bool]
    dockerfile_renku_version: Optional[str]
    latest_renku_version: str


@dataclass
class TemplateStatusResult:
    """Template migration status."""

    automated_template_update: bool
    newer_template_available: bool
    project_template_version: Optional[str]
    latest_template_version: Optional[str]
    template_source: Optional[str]
    template_ref: Optional[str]
    template_id: Optional[str]
    ssh_supported: bool


@dataclass
class MigrationCheckResult:
    """Migration check output."""

    project_supported: bool
    core_renku_version: str
    project_renku_version: Optional[str]
    core_compatibility_status: Union[CoreStatusResult, Exception]
    dockerfile_renku_status: Union[DockerfileStatusResult, Exception]
    template_status: Union[TemplateStatusResult, Exception]

    @staticmethod
    def from_minimum_version_error(minimum_version_error: MinimumVersionError) -> "MigrationCheckResult":
        """Create a migration check when the project isn't supported yet."""
        from renku import __version__

        return MigrationCheckResult(
            project_supported=False,
            core_renku_version=str(minimum_version_error.current_version),
            project_renku_version=f">={minimum_version_error.minimum_version}",
            core_compatibility_status=CoreStatusResult(
                migration_required=False,
                project_metadata_version=None,
                current_metadata_version=SUPPORTED_PROJECT_VERSION,
            ),
            dockerfile_renku_status=DockerfileStatusResult(
                dockerfile_renku_version="unknown",
                latest_renku_version=__version__,
                newer_renku_available=False,
                automated_dockerfile_update=False,
            ),
            template_status=TemplateStatusResult(
                automated_template_update=False,
                newer_template_available=False,
                template_source="unknown",
                template_ref="unknown",
                template_id="unknown",
                project_template_version="unknown",
                latest_template_version="unknown",
                ssh_supported=False,
            ),
        )


def migrations_check():
    """Return a command for a migrations check."""
    return Command().command(_migrations_check).with_database(write=False)


def _migrations_check() -> MigrationCheckResult:
    """Check migration status of project.

    Returns:
        Dictionary of project migrations, template and dockerfile status.
    """
    from renku.core.migration.migrate import is_project_unsupported

    core_version, latest_version = _migrations_versions()

    try:
        core_compatibility_status: Union[CoreStatusResult, Exception] = _metadata_migration_check()
    except Exception as e:
        core_compatibility_status = e

    try:
        docker_status: Union[DockerfileStatusResult, Exception] = _dockerfile_migration_check()
    except Exception as e:
        docker_status = e

    try:
        template_status: Union[TemplateStatusResult, Exception] = _template_migration_check()
    except Exception as e:
        template_status = e

    return MigrationCheckResult(
        project_supported=not is_project_unsupported(),
        core_renku_version=core_version,
        project_renku_version=latest_version,
        core_compatibility_status=core_compatibility_status,
        dockerfile_renku_status=docker_status,
        template_status=template_status,
    )


def migrations_versions():
    """Return a command to get source and destination migration versions."""
    return Command().command(_migrations_versions).lock_project().with_database()


def _migrations_versions() -> Tuple[str, Optional[str]]:
    """Return source and destination migration versions.

    Returns:
        Tuple of current version and project version.
    """
    from renku import __version__

    try:
        latest_agent = project_context.latest_agent
    except ValueError:
        # NOTE: maybe old project
        from renku.core.migration.utils import read_latest_agent

        latest_agent = read_latest_agent()

    return __version__, latest_agent


def _template_migration_check() -> TemplateStatusResult:
    """Return template migration status.

    Returns:
        Dictionary of template migration status.
    """
    from renku.core.config import get_value
    from renku.core.template.usecase import check_for_template_update

    try:
        project = project_context.project
    except ValueError:
        raise errors.MigrationRequired()

    template_source = project.template_metadata.template_source
    template_ref = project.template_metadata.template_ref
    template_id = project.template_metadata.template_id
    ssh_supported = project.template_metadata.ssh_supported

    ssh_supported = get_value("renku", "ssh_supported") == "true" or ssh_supported

    update_available, update_allowed, current_version, new_version = check_for_template_update(project)

    return TemplateStatusResult(
        automated_template_update=update_allowed,
        newer_template_available=update_available,
        project_template_version=current_version,
        latest_template_version=new_version,
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        ssh_supported=ssh_supported,
    )


def dockerfile_migration_check():
    """Return a command for a Dockerfile migrations check."""
    return Command().command(_dockerfile_migration_check)


def _dockerfile_migration_check() -> DockerfileStatusResult:
    """Return Dockerfile migration status.

    Returns:
        Dictionary of Dockerfile migration status.
    """
    from renku import __version__
    from renku.core.migration.migrate import update_dockerfile

    automated_dockerfile_update, newer_renku_available, dockerfile_renku_version = update_dockerfile(check_only=True)

    return DockerfileStatusResult(
        automated_dockerfile_update=automated_dockerfile_update,
        newer_renku_available=newer_renku_available,
        dockerfile_renku_version=dockerfile_renku_version,
        latest_renku_version=__version__,
    )


def metadata_migration_check():
    """Return a command for a metadata migrations check."""
    return Command().command(_metadata_migration_check)


def _metadata_migration_check() -> CoreStatusResult:
    """Return metadata migration status.

    Returns:
        Dictionary of metadata migration status.
    """
    from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION, get_project_version, is_migration_required

    return CoreStatusResult(
        migration_required=is_migration_required(),
        project_metadata_version=get_project_version(),
        current_metadata_version=SUPPORTED_PROJECT_VERSION,
    )


def migrate_project_command():
    """Return a command to migrate all project's entities."""
    from renku.core.migration.migrate import migrate_project

    return Command().command(migrate_project).lock_project().require_clean().with_database(write=True)


def check_project():
    """Return a command to check if repository is a renku project, unsupported, or requires migration."""
    return Command().command(_check_project).with_database(write=False)


def _check_project():
    from renku.core.migration.migrate import is_docker_update_possible, is_migration_required, is_project_unsupported
    from renku.core.template.usecase import check_for_template_update
    from renku.core.util.metadata import is_renku_project

    if not is_renku_project():
        return NON_RENKU_REPOSITORY
    elif is_project_unsupported():
        return UNSUPPORTED_PROJECT

    try:
        _ = project_context.project
    except ValueError:
        return MIGRATION_REQUIRED
    else:
        if hasattr(project_context.project, "template_source"):
            # NOTE: v10 migration not done
            return MIGRATION_REQUIRED

    # NOTE: ``project.automated_update`` is deprecated. We always allow template update for a project
    status = AUTOMATED_TEMPLATE_UPDATE_SUPPORTED

    if check_for_template_update(project_context.project)[0]:
        status |= TEMPLATE_UPDATE_POSSIBLE
    if is_docker_update_possible():
        status |= DOCKERFILE_UPDATE_POSSIBLE
    if is_migration_required():
        return status | MIGRATION_REQUIRED

    return status | SUPPORTED_RENKU_PROJECT


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def _check_immutable_template_files(paths: List[str]):
    """Check paths and return a list of those that are marked immutable in the project template.

    Args:
        paths(List[str]): Paths to check.

    Returns:
        List of immutable template files.
    """
    immutable_template_files = project_context.project.template_metadata.immutable_template_files or []

    return [p for p in paths if str(p) in immutable_template_files]


def check_immutable_template_files_command():
    """Command for checking immutable template files."""
    return Command().command(_check_immutable_template_files).with_database()
