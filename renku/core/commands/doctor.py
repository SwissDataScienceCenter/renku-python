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
"""Check your system and repository for potential problems."""
import traceback

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import ERROR

DOCTOR_INFO = """\
Please note that the diagnosis report is used to help Renku maintainers with
debugging if you file an issue. Use all proposed solutions with maximal care
and if in doubt ask an expert around or file an issue. Thanks!
"""


@pass_local_client
def doctor_check(client):
    """Check your system and repository for potential problems."""
    from . import checks

    is_ok = True
    problems = []

    for check in checks.__all__:
        try:
            ok, problems_ = getattr(checks, check)(client)
        except Exception:
            ok = False
            tb = "\n\t".join(traceback.format_exc().split("\n"))
            problems_ = f"{ERROR}Exception raised when running {check}\n\t{tb}"

        is_ok &= ok

        if problems_:
            problems.append(problems_)

    return is_ok, "\n".join(problems)
