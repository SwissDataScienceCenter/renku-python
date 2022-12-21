# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Renku run command."""

from typing import List, Optional

from renku.command.command_builder.command import Command
from renku.core.workflow.run import run_command_line
from renku.core.workflow.workflow_file import run_workflow_file


def run_command_line_command():
    """Run command line."""
    command = Command().command(run_command_line)
    return command.require_migration().require_clean().with_database(write=True).with_commit()


def run_workflow_file_command(no_commit: bool, commit_only: Optional[List[str]]):
    """Run a workflow file."""
    command = Command().command(run_workflow_file).require_migration()
    return (
        command.with_database(write=False)
        if no_commit
        else command.with_database(write=True).with_commit(
            commit_only=commit_only, skip_staging=True, skip_dirty_checks=True
        )
    )
