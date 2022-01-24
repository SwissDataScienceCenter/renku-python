# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Renku environment related commands."""

import click


@click.command()
@click.option("shell_completion", "--shell-completion", is_flag=True, help="Print shell completion command")
@click.pass_context
def env(ctx, shell_completion):
    """Renku environment commands."""

    if shell_completion:
        import shellingham

        from renku.core.errors import UsageError

        shell = shellingham.detect_shell()[0]
        if shell == "bash":
            click.echo("_RENKU_COMPLETE=bash_source renku")
        elif shell == "zsh":
            click.echo("_RENKU_COMPLETE=zsh_source renku")
        elif shell == "fish":
            click.echo("env _RENKU_COMPLETE=fish_source renku")
        else:
            raise UsageError(f"The currently used shell '{shell}' is not supported for shell completion.")
    else:
        click.echo(ctx.get_help())
