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
"""Template management commands."""

from renku.command.command_builder.command import Command
from renku.core.template.usecase import list_templates, set_template, show_template, update_template, validate_templates


def list_templates_command():
    """Command to list available templates."""
    return Command().command(list_templates)


def show_template_command():
    """Command to show template details."""
    return Command().command(show_template).with_database()


def set_template_command():
    """Command to set template for a project."""
    return (
        Command()
        .command(set_template)
        .lock_project()
        .require_migration()
        .require_clean()
        .with_database(write=True)
        .with_commit()
    )


def update_template_command():
    """Command to update project's template."""
    return (
        Command()
        .command(update_template)
        .lock_project()
        .require_migration()
        .require_clean()
        .with_database(write=True)
        .with_commit()
    )


def validate_templates_command():
    """Command to validate a template repository."""
    return Command().command(validate_templates)
