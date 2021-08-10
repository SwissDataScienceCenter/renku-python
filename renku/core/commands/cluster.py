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

import os
from pathlib import Path
import requests
from subprocess import PIPE, run, SubprocessError
from urllib.parse import quote

from jinja2 import Template
from pkg_resources import resource_filename

from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.models.git import GitURL
from renku.core.utils import communication
from renku.core.utils.urls import get_remote

CONFIG_SECTION = "cluster"
IMAGE_SHA = 7
IMAGE_SUFFIX = "-batch"
DEFAULT_TEMPLATE = resource_filename("renku", "data/default_sbatch_script.sh.tpl")


@inject.autoparams()
def _prepare_git(client: LocalClient):
    """Read Git configurations."""
    communication.info("Prepare Git configurations...")
    if not client.repo or not client.repo.remotes:
        raise errors.GitError("Cannot find a remote Git repository.")
    git_config = client.repo.config_reader()
    git_username = git_config.get_value("user", "name", None)
    git_email = git_config.get_value("user", "email", None)
    if not git_username or not git_email:
        raise errors.GitError("Git username or email is not set up for this repository.")
    _, git_remote = get_remote(client.repo)
    config = {
        "git_username": git_username,
        "git_email": git_email,
        "git_remote": git_remote,
    }
    for k, v in config.items():
        communication.echo(f"{k}: {v}")
        client.set_value(section=CONFIG_SECTION, key=k, value=v)


def _prompt_gitlab_token(token_url):
    text_prompt = "You must configure a personal access token for Renku GitLab.\n"
    text_prompt += f"Create one at {token_url}\n"
    text_prompt += "Please choose **api** scope.\n"
    text_prompt += "Your token"
    return communication.prompt(text_prompt, type=str, hide_input=True)


@inject.autoparams()
def _prepare_gitlab_token(client: LocalClient, gitlab_token):
    """Set up renku gitlab token in global configuration."""
    communication.info("Prepare Renku GitLab token...")
    if not gitlab_token:
        # check if value already exists
        if client.get_value(CONFIG_SECTION, "gitlab_token"):
            # ?: promopt for confirmation?
            communication.echo("Found Renku GitLab token in global renku.ini.")
            return
        # prompt for user to input token
        token_url = "https://" + client.remote["host"] + "/gitlab/-/profile/personal_access_tokens"
        gitlab_token = _prompt_gitlab_token(token_url)
        # ?: check token scope before saving it?
    # save the token in global configuration
    client.set_value(section=CONFIG_SECTION, key="gitlab_token", value=gitlab_token, global_only=True)
    os.chmod(client.global_config_path, 0o600)
    communication.echo("Token saved in global renku.ini.")


def _get_image_uri(remote, tag, token):
    """Use GitLab API to check if image with tag exists."""
    url = GitURL.parse(remote)
    api_base, namespace, project_name = url.api_base, url.owner, url.name
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
def _prepare_renku_image(client: LocalClient):
    """Detect the latest batch image in registry."""
    communication.info("Prepare image to be used for batch execution...")
    # read values configuration
    remote = client.get_value(section=CONFIG_SECTION, key="git_remote")
    token = client.get_value(section=CONFIG_SECTION, key="gitlab_token")
    # get latest SHA in remote
    commit_sha = client.repo.head.reference.tracking_branch().commit.hexsha
    tag = commit_sha[:IMAGE_SHA] + IMAGE_SUFFIX
    try:
        sbatch_image = _get_image_uri(remote, tag, token)
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 401:
            client.remove_value(CONFIG_SECTION, "gitlab_token", global_only=True)
            raise errors.AuthenticationError("Token unauthorized for Renku GitLab. Remove it from global renku.ini.")
        if err.response.status_code == 403:
            client.remove_value(CONFIG_SECTION, "gitlab_token", global_only=True)
            raise errors.APIError("Token with wrong scope. Remove it from global renku.ini.")
        if err.response.status_code == 404:
            raise errors.APIError(f"Cannot find corresponding image {tag}. Please check GitLab pipeline logs.")
        raise errors.APIError(err)
    communication.echo(f"sbatch_image: {sbatch_image}")
    client.set_value(section=CONFIG_SECTION, key="sbatch_image", value=sbatch_image)


@inject.autoparams()
def _prepare_sbatch(client: LocalClient, sbatch_options, sbatch_template, sbatch_script):
    """Set up sbatch options and template."""
    communication.info("Prepare sbatch options and template...")
    # if sbatch_template doesn't exist, use the default one
    if not Path(sbatch_template).exists():
        communication.warn(f"Template {sbatch_template} doesn't exist in current repoistory. Use default one instead.")
        default_template = Path(DEFAULT_TEMPLATE)
        # name of the template
        sbatch_template = default_template.name
        Path(sbatch_template).write_text(default_template.read_text())
    # if sbatch_options is empty, check if there is existed one
    if not sbatch_options:
        sbatch_options_old = client.get_value(section=CONFIG_SECTION, key="sbatch_options")
        if sbatch_options_old:
            communication.info(f"Use existed sbatch options: {sbatch_options_old}.")
            sbatch_options = sbatch_options_old
        else:
            communication.warn("Empty sbatch options.")
    config = {"sbatch_template": sbatch_template, "sbatch_script": sbatch_script, "sbatch_options": sbatch_options}
    for k, v in config.items():
        communication.echo(f"{k}: {v}")
        client.set_value(section=CONFIG_SECTION, key=k, value=v)


@inject.autoparams()
def _prepare_cluster_config(client: LocalClient, sbatch_options, sbatch_template, sbatch_script, gitlab_token):
    """Set up configuration for cluster execution"""
    _prepare_git(client)
    _prepare_gitlab_token(client, gitlab_token)
    _prepare_renku_image(client)
    _prepare_sbatch(client, sbatch_options, sbatch_template, sbatch_script)
    text_info = "Configurations have been saved in renku.ini.\n"
    text_info += "To view all configurations, you could run (remove --local to include token)\n"
    text_info += "      renku config show --local\n"
    text_info += "To change some specific configuration, you could run\n"
    text_info += "      renku config set cluster.<KEY> <VALUE>\n"
    text_info += "e.g.  renku config set cluster.sbatch_options 'partition=debug nodes=2'"
    communication.info(text_info)


def prepare_cluster_config():
    return Command().command(_prepare_cluster_config).with_commit(commit_if_empty=False)


def _read_template(file):
    return Template(Path(file).read_text())


def _write_script(file, script):
    Path(file).write_text(script)


@inject.autoparams()
def _compose_sbatch_script(client: LocalClient, command):
    """Compose sbatch script."""
    # read cluster configuration
    cluster_config = client.get_section(CONFIG_SECTION)
    if not cluster_config:
        raise errors.ConfigurationError("Cluster configurations are empty. Please run: renku cluster prepare")
    if "gitlab_token" not in cluster_config:
        raise errors.ConfigurationError("Found no Renku GitLab token. Please run: renku cluster prepare")
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
    try:
        sbatch_run = run(
            ["sbatch", script],
            env=dict(os.environ, RENKU_TOKEN=token),
            stdout=PIPE,
            stderr=PIPE,
            check=True,
            cwd=client.path,
            universal_newlines=True,
        )
    except FileNotFoundError:
        raise errors.SbatchError("Please check if you have sbatch executable.")
    except SubprocessError as e:
        raise errors.SbatchError(e.stderr)
    return sbatch_run.stdout


@inject.autoparams()
def _execute_cluster_command(client: LocalClient, command):
    """Excute command by submitting a sbatch script."""
    _compose_sbatch_script(client, command)
    sbatch_output = _submit_sbatch_script(client)
    return sbatch_output


def execute_cluster_command():
    return Command().command(_execute_cluster_command).with_commit()
