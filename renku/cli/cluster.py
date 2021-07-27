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

from renku.core.commands.cluster import prepare_cluster_config, execute_cluster_command


@click.group()
def cluster():
    """Cluster commands."""


@cluster.command("prepare")
@click.option("--token", "gitlab_token", required=True, metavar="TOKEN", help="Token for accessing GitLab.")
@click.option("--options", "sbatch_options", required=True, metavar="'K1=V1 K2 ...'", help="Set sbatch options.")
def prepare(sbatch_options, gitlab_token):
    """Prepare options used in cluster execution."""
    # ?: should use renku login to generate the token?
    prepare_cluster_config().build().execute(sbatch_options=sbatch_options, gitlab_token=gitlab_token)
    click.secho("OK", fg="green")


@cluster.command("update", context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def update(args):
    # TODO: check the validity of arguments
    command = " ".join(("renku", "update") + args)
    value = execute_cluster_command().build().execute(command)
    click.secho(value.output)
    click.secho("OK", fg="green")


@cluster.command("run")
def run():
    pass


@cluster.command("rerun")
def rerun():
    pass
