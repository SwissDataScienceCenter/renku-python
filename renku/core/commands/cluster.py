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

import click
import os
from pathlib import Path
import requests
from subprocess import PIPE, STDOUT, run
from urllib.parse import quote

from jinja2 import Template
from pkg_resources import resource_filename

from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.models.git import GitURL
from renku.core.utils.urls import get_remote

CONFIG_SECTION = "cluster"
IMAGE_SHA = 7
IMAGE_SUFFIX = "batch"
DEFAULT_TEMPLATE = resource_filename("renku", "data/default_sbatch_template.sh")


@inject.autoparams()
def _prepare_git(client: LocalClient):
    click.secho("Read Git configuration...", fg="yellow")
    if not client.repo or not client.repo.remotes:
        raise errors.GitError("Cannot find a remote Git repository.")
    git_config = client.repo.config_reader()
    git_username = git_config.get_value("user", "name", None)
    git_email = git_config.get_value("user", "email", None)
    if not git_username or not git_email:
        raise errors.GitError("Git username or email is not set up for this repository")
    _, git_remote = get_remote(client.repo)
    config = {
        "git_username": git_username,
        "git_email": git_email,
        "git_remote": git_remote,
    }
    for k, v in config.items():
        click.echo(f"{k}: {v}")
        client.set_value(section=CONFIG_SECTION, key=k, value=v)


@inject.autoparams()
def _prepare_token(client: LocalClient, gitlab_token):
    click.secho("Store Renku GitLab token...", fg="yellow")
    client.set_value(section=CONFIG_SECTION, key="gitlab_token", value=gitlab_token, global_only=True)
    os.chmod(client.global_config_path, 0o600)


def _get_image_uri(api_base, namespace, project_name, tag, token):
    # get repository id in project's container registry
    id_ = quote(namespace + "/" + project_name, safe="")
    res = requests.get(api_base + f"/projects/{id_}/registry/repositories", headers={"PRIVATE-TOKEN": token})
    res.raise_for_status()
    repo_id = res.json()[0]["id"]
    # get image uri with the specific tag
    res = requests.get(
        api_base + f"/projects/{id_}/registry/repositories/{repo_id}/tags/{tag}", headers={"PRIVATE-TOKEN": token}
    )
    res.raise_for_status()
    return res.json()["location"]


@inject.autoparams()
def _prepare_image(client: LocalClient):
    click.secho("Check image to be used...", fg="yellow")
    # read values configuration
    remote = client.get_value(section=CONFIG_SECTION, key="git_remote")
    token = client.get_value(section=CONFIG_SECTION, key="gitlab_token")
    # parse Git remote url
    url = GitURL.parse(remote)
    api_base, namespace, project_name = url.api_base, url.owner, url.name
    # get latest SHA in remote
    commit_sha = client.repo.head.reference.tracking_branch().commit.hexsha
    tag = commit_sha[:IMAGE_SHA] + "-" + IMAGE_SUFFIX
    try:
        sbatch_image = _get_image_uri(api_base, namespace, project_name, tag, token)
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 401:
            raise errors.APIError("Unauthorized Renku GitLab token.")
        if err.response.status_code == 404:
            raise errors.APIError(f"Cannot find corresponding image {tag}. Please check GitLab pipeline logs.")
        raise errors.APIError(err)
    click.echo(f"sbatch_image: {sbatch_image}")
    client.set_value(section=CONFIG_SECTION, key="sbatch_image", value=sbatch_image)


@inject.autoparams()
def _prepare_sbatch(client: LocalClient, sbatch_options, sbatch_template, sbatch_script):
    click.secho("Set up sbatch...", fg="yellow")
    if not Path(sbatch_template).exists():
        click.echo(f"Template {sbatch_template} doesn't exist in current repoistory. Use default one instead.")
        default_template = Path(DEFAULT_TEMPLATE)
        Path(sbatch_template).write_text(default_template.read_text())
    config = {"sbatch_template": sbatch_template, "sbatch_script": sbatch_script, "sbatch_options": sbatch_options}
    for k, v in config.items():
        click.echo(f"{k}: {v}")
        client.set_value(section=CONFIG_SECTION, key=k, value=v)


@inject.autoparams()
def _prepare_cluster_config(client: LocalClient, gitlab_token, sbatch_options, sbatch_template, sbatch_script):
    """Set up configuration for cluster execution"""
    _prepare_token(client, gitlab_token)
    _prepare_git(client)
    _prepare_image(client)
    _prepare_sbatch(client, sbatch_options, sbatch_template, sbatch_script)


def prepare_cluster_config():
    return Command().command(_prepare_cluster_config).with_commit(commit_if_empty=False)


def _read_template(file):
    return Template(Path(file).read_text())


def _write_script(file, script):
    Path(file).write_text(script)


@inject.autoparams()
def _compose_sbatch_script(client: LocalClient, command):
    """Compose sbatch script."""
    # TODO: add error handling if prepare command hasn't yet been run
    # read cluster configuration
    cluster_config = client.get_section(CONFIG_SECTION)
    # add renku commnand
    cluster_config["renku_command"] = command
    # read template
    sbatch_template = _read_template(cluster_config["sbatch_template"])
    # render template
    sbatch_script = sbatch_template.render(cluster_config)
    # write script
    _write_script(cluster_config["sbatch_script"], sbatch_script)


@inject.autoparams()
def _submit_sbatch_script(client: LocalClient):
    """Submit sbatch script."""
    script = client.get_value(section=CONFIG_SECTION, key="sbatch_script")
    token = client.get_value(section=CONFIG_SECTION, key="gitlab_token")
    # TODO: add error handling
    sbatch_run = run(
        ["sbatch", script],
        env=dict(os.environ, RENKU_TOKEN=token),
        stdout=PIPE,
        stderr=STDOUT,
        cwd=client.path,
        universal_newlines=True,
    )
    return sbatch_run.stdout


@inject.autoparams()
def _execute_cluster_command(client: LocalClient, command):
    """Excute command by submitting a sbatch script."""
    _compose_sbatch_script(client, command)
    sbatch_output = _submit_sbatch_script(client)
    return sbatch_output


def execute_cluster_command():
    return Command().command(_execute_cluster_command).with_commit()
