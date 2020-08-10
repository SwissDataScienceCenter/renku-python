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
"""Check missing references."""

import click

from ..echo import WARNING


def check_missing_references(client):
    """Find missing references."""
    from renku.core.models.refs import LinkReference

    missing = [ref for ref in LinkReference.iter_items(client) if not ref.reference.exists()]

    if not missing:
        return True, None

    problems = (
        "\n" + WARNING + "There are missing references."
        '\n  (use "git rm <name>" to clean them)\n\n\t'
        + "\n\t".join(
            click.style(str(ref.path), fg="yellow") + " -> " + click.style(str(ref.reference), fg="red")
            for ref in missing
        )
        + "\n"
    )
    return False, problems
