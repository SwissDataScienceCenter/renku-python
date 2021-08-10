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
"""Prepare and batch execute Renku workflow.

TODO
"""

import click
from renku.cli.utils.callback import ClickCallback
from renku.core.commands.cluster import prepare_cluster_config, execute_cluster_command


@click.group()
def cluster():
    """Cluster commands."""


@cluster.command("prepare")
@click.option(
    "--options",
    "sbatch_options",
    metavar="'K1 K2=V2 ...'",
    default="",
    help="Options for sbatch script (if empty, use the previously set options).",
)
@click.option(
    "--template",
    "sbatch_template",
    default="sbatch_script.sh.tpl",
    required=False,
    help="Script template.",
    show_default=True,
)
@click.option(
    "--script",
    "sbatch_script",
    default="sbatch_script.sh",
    required=False,
    help="Script to be created.",
    show_default=True,
)
@click.option("--token", "gitlab_token", metavar="TOKEN", help="Token for Renku GitLab.")
def prepare(sbatch_options, sbatch_script, sbatch_template, gitlab_token):
    """Prepare cluster execution."""
    # ?: should use renku login to generate the token?
    communicator = ClickCallback()
    prepare_cluster_config().with_communicator(communicator).build().execute(
        sbatch_options=sbatch_options,
        sbatch_template=sbatch_template,
        sbatch_script=sbatch_script,
        gitlab_token=gitlab_token,
    )
    click.secho("OK", fg="green")


@cluster.command("update", context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def update(args):
    # TODO: check the validity of arguments
    # TODO: pre-execution check of renku-update (if path exists, for example)
    command = " ".join(("renku", "update") + args)
    value = execute_cluster_command().build().execute(command=command)
    click.secho(value.output)
    click.secho("OK", fg="green")


@cluster.command("run")
def run():
    pass


@cluster.command("rerun")
def rerun():
    pass
