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
"""Install and uninstall Git hooks."""

from renku.command.command_builder.command import Command
from renku.core.githooks import install_githooks, uninstall_githooks


def install_githooks_command():
    """Command to install Git hooks."""
    return Command().command(install_githooks)


def uninstall_githooks_command():
    """Command to uninstall Git hooks."""
    return Command().command(uninstall_githooks)
