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
"""Renku cluster command."""

from pathlib import Path
from subprocess import PIPE, STDOUT, run

from jinja2 import Template

from renku.core.incubation.command import Command
from renku.core.management.config import CONFIG_LOCAL_PATH
from renku.core.commands.docker import detect_registry_url

CONFIG_SECTION = "cluster"
IMAGE_SHA = 7


def _prepare_cluster_config(client, sbatch_options, gitlab_token):
    """Set up configuration for cluster execution"""
    registry_url = detect_registry_url(client, auto_login=False)
    # get the latest commit SHA at remote
    renku_commit = client.repo.head.reference.tracking_branch().commit.hexsha
    git_config = client.repo.config_reader()
    cluster_config = {
        # default
        "sbatch_template": "sbatch_script_template.sh",
        "sbatch_script": "sbatch_script.sh",
        # user input
        "sbatch_options": sbatch_options,
        "gitlab_token": gitlab_token,
        # read from repo
        "git_username": git_config.get_value("user", "name", None),
        "git_email": git_config.get_value("user", "email", None),
        "git_remote": registry_url.href,
        "sbatch_image": f"{registry_url.image}/{renku_commit[:IMAGE_SHA]}-batch",
    }
    for k, v in cluster_config.items():
        client.set_value(section=CONFIG_SECTION, key=k, value=v)


def prepare_cluster_config():
    return Command().command(_prepare_cluster_config).with_commit(commit_if_empty=False, commit_only=CONFIG_LOCAL_PATH)


def _read_cluster_config(client):
    """Read cluster section of config."""
    config = client.get_config(as_string=False)
    return {k.partition(".")[2]: v for k, v in config.items() if k.partition(".")[0] == CONFIG_SECTION}


def _read_template(file):
    return Template(Path(file).read_text())


def _write_script(file, script):
    Path(file).write_text(script)


def _compose_sbatch_script(client, command):
    """Compose sbatch script."""
    # read cluster configuration
    cluster_config = _read_cluster_config(client)
    # add renku commnand
    cluster_config["renku_command"] = command
    # read template
    sbatch_template = _read_template(cluster_config["sbatch_template"])
    # render template
    sbatch_script = sbatch_template.render(cluster_config)
    # write script
    _write_script(cluster_config["sbatch_script"], sbatch_script)


def _submit_sbatch_script(client):
    """Submit sbatch script."""
    script = client.get_value(section=CONFIG_SECTION, key="sbatch_script")
    # TODO: add error handling
    sbatch_run = run(["sbatch", script], stdout=PIPE, stderr=STDOUT, cwd=client.path, universal_newlines=True)
    return sbatch_run.stdout


def _execute_cluster_command(client, command):
    _compose_sbatch_script(client, command)
    sbatch_output = _submit_sbatch_script(client)
    return sbatch_output


def execute_cluster_command():
    return Command().command(_execute_cluster_command).with_commit()
