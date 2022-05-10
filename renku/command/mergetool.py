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
"""Custom git mergetool for renku metadata."""

from pathlib import Path

from renku.command.command_builder import inject
from renku.command.command_builder.command import Command
from renku.core.interface.client_dispatcher import IClientDispatcher


def mergetool_command():
    """Command to move or rename a file, a directory, or a symlink."""
    return Command().command(_mergetool).require_migration().with_database()


@inject.autoparams()
def _mergetool(local: Path, remote: Path, base: Path) -> None:
    """Merge renku metadata files.

    Args:
        local(Path): The file to merge from the local branch.
        remote(Path): The file to merge from the remote branch.
        base(Path): Path to common base of branches to be merged.
    """
    from renku.infrastructure.git_merger import GitMerger

    merger = GitMerger()

    merger.merge(local, remote, base)


def mergetool_install_command():
    """Command to setup renku as a custom git merge tool."""
    return Command().command(setup_mergetool)


@inject.autoparams("client_dispatcher")
def setup_mergetool(client_dispatcher: IClientDispatcher, with_attributes: bool = True):
    """Setup renku custom mergetool."""
    client = client_dispatcher.current_client

    with client.repository.get_configuration(writable=True) as config_writer:
        config_writer.set_value('merge "renkumerge"', "name", "Renku merge driver")
        config_writer.set_value('merge "renkumerge"', "driver", "renku mergetool merge %O %A %B")
        config_writer.set_value('merge "renkumerge"', "trustExitCode", "true")
        config_writer.set_value('merge "renkumerge"', "recursive", "binary")

    if not with_attributes:
        return

    attributes_path = client.path / ".gitattributes"
    pattern_string = ".renku/metadata/**    merge=renkumerge\n"

    if attributes_path.exists():
        with open(attributes_path, "r") as f:
            content = f.readlines()
            if pattern_string in content:
                return

    with open(attributes_path, "a") as f:
        f.write(pattern_string)
