# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
of these files has m_1234__name.py format where 1234 is the migration version
and name can be any alphanumeric and underscore combination. Migration files
are sorted based on their lowercase name. Each migration file must define a
public "migrate" function that accepts a client as its argument.

When executing a migration, the migration file is imported as a module and the
"migrate" function is executed. Migration version is checked against the Renku
project version (in .renku/metadata.yml) and any migration which has a higher
version is applied to the project.
"""
import hashlib
import importlib
import json
import os
import re
import shutil
from pathlib import Path

import pkg_resources
from jinja2 import Template

from renku.core.errors import (
    DockerfileUpdateError,
    MigrationError,
    MigrationRequired,
    ProjectNotSupported,
    TemplateUpdateError,
)
from renku.core.utils import communication
from renku.core.utils.migrate import read_project_version

SUPPORTED_PROJECT_VERSION = 8


def check_for_migration(client):
    """Checks if migration is required."""
    if is_migration_required(client):
        raise MigrationRequired
    elif is_project_unsupported(client):
        raise ProjectNotSupported


def is_migration_required(client):
    """Check if project requires migration."""
    return is_renku_project(client) and _get_project_version(client) < SUPPORTED_PROJECT_VERSION


def is_project_unsupported(client):
    """Check if this version of Renku cannot work with the project."""
    return is_renku_project(client) and _get_project_version(client) > SUPPORTED_PROJECT_VERSION


def is_template_update_possible(client):
    """Check if the project can be updated to a newer version of the project template."""
    return _update_template(client, check_only=True)


def is_docker_update_possible(client):
    """Check if the Dockerfile can be updated to a new version of renku-python."""
    return _update_dockerfile(client, check_only=True)


def migrate(
    client,
    force_template_update=False,
    skip_template_update=False,
    skip_docker_update=False,
    skip_migrations=False,
    project_version=None,
):
    """Apply all migration files to the project."""
    template_updated = docker_updated = False
    if not is_renku_project(client):
        return False, template_updated, docker_updated

    if (
        not skip_template_update
        and client.project.template_source
        and (force_template_update or client.project.automated_update)
    ):
        try:
            template_updated, _, _ = _update_template(client)
        except TemplateUpdateError:
            raise
        except (Exception, BaseException) as e:
            raise TemplateUpdateError("Couldn't update from template.") from e

    if not skip_docker_update:
        try:
            docker_updated = _update_dockerfile(client)
        except DockerfileUpdateError:
            raise
        except (Exception, BaseException) as e:
            raise DockerfileUpdateError("Couldn't update renku version in Dockerfile.") from e

    if skip_migrations:
        return False, template_updated, docker_updated

    project_version = project_version or _get_project_version(client)
    n_migrations_executed = 0

    version = 1
    for version, path in get_migrations():
        if version > project_version:
            module = importlib.import_module(path)
            module_name = module.__name__.split(".")[-1]
            communication.echo(f"Applying migration {module_name}...")
            try:
                module.migrate(client)
            except (Exception, BaseException) as e:
                raise MigrationError("Couldn't execute migration") from e
            n_migrations_executed += 1
    if n_migrations_executed > 0 and not client.is_using_temporary_datasets_path():
        client._project = None  # NOTE: force reloading of project metadata
        client.project.version = str(version)
        client.project.to_yaml()

        communication.echo(f"Successfully applied {n_migrations_executed} migrations.")

    return n_migrations_executed != 0, template_updated, docker_updated


def _update_template(client, check_only=False):
    """Update local files from the remote template."""
    from renku.core.commands.init import fetch_template

    project = client.project

    if not project.template_version:
        return False, None, None

    template_manifest, template_folder, template_source, template_version = fetch_template(
        project.template_source, project.template_ref
    )

    if template_source == "renku":
        template_version = pkg_resources.parse_version(template_version)
        current_version = pkg_resources.parse_version(project.template_version)
        if template_version <= current_version:
            return False, project.template_version, current_version
    else:
        if template_version == project.template_version:
            return False, project.template_version, template_version

    if check_only:
        return True, project.template_version, template_version

    communication.echo("Updating project from template...")

    template_filtered = [
        template_elem for template_elem in template_manifest if template_elem["folder"] == project.template_id
    ]
    if len(template_filtered) == 1:
        template_data = template_filtered[0]
    else:
        raise TemplateUpdateError(f'The template with id "{project.template_id}" is not available.')

    template_path = template_folder / template_data["folder"]

    metadata = json.loads(project.template_metadata)

    template_variables = set(template_data.get("variables", {}).keys())
    metadata_keys = set(metadata.keys())
    missing_keys = ", ".join(template_variables - metadata_keys)
    if missing_keys:
        raise TemplateUpdateError(
            f"Can't update template, it now requires variable(s) {missing_keys} which were not present on project "
            "creation."
        )

    if not os.path.exists(client.template_checksums):
        raise TemplateUpdateError("Can't update template as there are no template checksums set on the project.")

    with open(client.template_checksums, "r") as checksum_file:
        checksums = json.load(checksum_file)

    updated_files = []

    for file in template_path.glob("**/*"):
        rel_path = file.relative_to(template_path)
        destination = client.path / rel_path

        # NOTE: the path could contain template variables, we need to template it
        destination = Path(Template(str(destination)).render(metadata))

        try:
            # parse file and process it
            template = Template(file.read_text())
            rendered_content = template.render(metadata)
            sha256_hash = hashlib.sha256()
            content_bytes = rendered_content.encode("utf-8")
            blocksize = 4096
            blocks = (len(content_bytes) - 1) // blocksize + 1
            for i in range(blocks):
                byte_block = content_bytes[i * blocksize : (i + 1) * blocksize]
                sha256_hash.update(byte_block)
            new_template_hash = sha256_hash.hexdigest()

            if not destination.exists() and str(rel_path) not in checksums:
                # NOTE: new file in template
                local_changes = False
                remote_changes = True
            else:
                current_hash = None  # NOTE: None if user deleted file locally

                if destination.exists():
                    current_hash = client._content_hash(destination)

                local_changes = current_hash != checksums[str(rel_path)]
                remote_changes = new_template_hash != checksums[str(rel_path)]

            if local_changes:
                if remote_changes and str(rel_path) in project.immutable_template_files:
                    # NOTE: There are local changes in a file that should not be changed by users,
                    # and the file was updated in the template as well. So the template can't be updated.
                    raise TemplateUpdateError(
                        f"Can't update template as immutable template file {rel_path} has local changes."
                    )
                continue
            elif not remote_changes:
                continue

            destination.write_text(rendered_content)
        except IsADirectoryError:
            destination.mkdir(parents=True, exist_ok=True)
        except TypeError:
            shutil.copy(file, destination)

    updated = "\n".join(updated_files)
    communication.echo(f"Updated project from template, updated files:\n{updated}")

    project.template_version = template_version
    project.to_yaml()

    return True, project.template_version, template_version


def _update_dockerfile(client, check_only=False):
    """Update the dockerfile to the newest version of renku."""
    from renku import __version__

    if not client.docker_path.exists():
        return False

    communication.echo("Updating dockerfile...")

    with open(client.docker_path, "r") as f:
        dockercontent = f.read()

    current_version = pkg_resources.parse_version(__version__)
    m = re.search(r"^ARG RENKU_VERSION=(\d+\.\d+\.\d+)$", dockercontent, flags=re.MULTILINE)
    if not m:
        if check_only:
            return False
        raise DockerfileUpdateError(
            "Couldn't update renku-python version in Dockerfile, as it doesn't contain an 'ARG RENKU_VERSION=...' line."
        )

    docker_version = pkg_resources.parse_version(m.group(1))

    if docker_version >= current_version:
        return False

    if check_only:
        return True

    dockercontent = re.sub(
        r"^ARG RENKU_VERSION=\d+\.\d+\.\d+$", f"ARG RENKU_VERSION={__version__}", dockercontent, flags=re.MULTILINE,
    )

    with open(client.docker_path, "w") as f:
        f.write(dockercontent)

    communication.echo("Updated dockerfile.")

    return True


def _get_project_version(client):
    try:
        return int(read_project_version(client))
    except ValueError:
        return 1


def is_renku_project(client):
    """Check if repository is a renku project."""
    try:
        return client.project is not None
    except ValueError:  # Error in loading due to an older schema
        return client.renku_metadata_path.exists()


def get_migrations():
    """Return a sorted list of versions and migration modules."""
    migrations = []
    for file_ in pkg_resources.resource_listdir("renku.core.management", "migrations"):
        match = re.search(r"m_([0-9]{4})__[a-zA-Z0-9_-]*.py", file_)

        if match is None:  # migration files match m_0000__[name].py format
            continue

        version = int(match.groups()[0])
        path = "renku.core.management.migrations.{}".format(Path(file_).stem)
        migrations.append((version, path))

    migrations = sorted(migrations, key=lambda v: v[1].lower())
    return migrations
