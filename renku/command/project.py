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
"""Project management."""

from renku.command.command_builder.command import Command
from renku.core.constant import DATABASE_METADATA_PATH
from renku.core.project import edit_project, show_project


def edit_project_command():
    """Command for editing project metadata."""
    command = Command().command(edit_project).lock_project().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATABASE_METADATA_PATH)


def show_project_command():
    """Command for showing project metadata."""
    return Command().command(show_project).lock_project().with_database().require_migration()
