# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Interactive session business logic."""

import os
import shutil
import textwrap
from pathlib import Path
from typing import List, NamedTuple, Optional

from pydantic import ConfigDict, validate_call

from renku.core import errors
from renku.core.config import get_value
from renku.core.plugin.session import get_supported_hibernating_session_providers, get_supported_session_providers
from renku.core.session.utils import get_image_repository_host, get_renku_project_name
from renku.core.util import communication
from renku.core.util.os import safe_read_yaml
from renku.core.util.ssh import SystemSSHConfig, generate_ssh_keys
from renku.domain_model.session import IHibernatingSessionProvider, ISessionProvider, Session, SessionStopStatus


def _safe_get_provider(provider: str) -> ISessionProvider:
    try:
        return next(p for p in get_supported_session_providers() if p.name == provider)
    except StopIteration:
        raise errors.ParameterError(f"Session provider '{provider}' is not available!")


class SessionList(NamedTuple):
    """Session list return."""

    sessions: List[Session]
    all_local: bool
    warning_messages: List[str]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def search_sessions(name: str, provider: Optional[str] = None) -> List[str]:
    """Get all sessions that their name starts with the given name.

    Args:
        name(str): The name to search for.
        provider(Optional[str]): Name of the session provider to use (Default value = None).

    Returns:
        All sessions whose name starts with ``name``.
    """
    sessions = session_list(provider=provider).sessions
    name = name.lower()
    return [s.id for s in sessions if s.id.lower().startswith(name)]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def search_session_providers(name: str) -> List[str]:
    """Get all session providers that their name starts with the given name.

    Args:
        name(str): The name to search for.

    Returns:
        All session providers whose name starts with ``name``.
    """
    from renku.core.plugin.session import get_supported_session_providers

    name = name.lower()
    return [p.name for p in get_supported_session_providers() if p.name.lower().startswith(name)]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def search_hibernating_session_providers(name: str) -> List[str]:
    """Get all session providers that support hibernation and their name starts with the given name.

    Args:
        name(str): The name to search for.

    Returns:
        All session providers whose name starts with ``name``.
    """
    from renku.core.plugin.session import get_supported_hibernating_session_providers

    name = name.lower()
    return [p.name for p in get_supported_hibernating_session_providers() if p.name.lower().startswith(name)]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_list(*, provider: Optional[str] = None) -> SessionList:
    """List interactive sessions.

    Args:
        provider(Optional[str]): Name of the session provider to use (Default value = None).

    Returns:
        The list of sessions, whether they're all local sessions and potential warnings raised.
    """

    def list_sessions(session_provider: ISessionProvider) -> List[Session]:
        try:
            return session_provider.session_list(project_name=project_name)
        except errors.RenkulabSessionGetUrlError:
            if provider:
                raise
            return []

    project_name = get_renku_project_name()
    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()

    all_sessions = []
    warning_messages = []
    all_local = True
    for session_provider in sorted(providers, key=lambda p: p.priority):
        try:
            sessions = list_sessions(session_provider)
        except errors.RenkuException as e:
            warning_messages.append(f"Cannot get sessions list from '{session_provider.name}': {e}")
        else:
            if session_provider.is_remote_provider():
                all_local = False
            all_sessions.extend(sessions)

    return SessionList(all_sessions, all_local, warning_messages)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_start(
    config_path: Optional[str],
    provider: str,
    image_name: Optional[str] = None,
    cpu_request: Optional[float] = None,
    mem_request: Optional[str] = None,
    disk_request: Optional[str] = None,
    gpu_request: Optional[str] = None,
    **kwargs,
):
    """Start interactive session.

    Args:
        config_path(str, optional): Path to config YAML.
        provider(str, optional): Name of the session provider to use.
        image_name(str, optional): Image to start.
        cpu_request(float, optional): Number of CPUs to request.
        mem_request(str, optional): Size of memory to request.
        disk_request(str, optional): Size of disk to request (if supported by provider).
        gpu_request(str, optional): Number of GPUs to request.
    """
    from renku.domain_model.project_context import project_context

    if project_context.repository.head.detached:
        raise errors.SessionStartError("Cannot start a session from a detached HEAD. Check out a branch first.")

    # NOTE: The Docker client in Python requires the parameters below to be a list and will fail with a tuple.
    # Click will convert parameters with the flag "many" set to True to tuples.
    kwargs["security_opt"] = list(kwargs.get("security_opt", []))
    kwargs["device_cgroup_rules"] = list(kwargs.get("device_cgroup_rules", []))

    pinned_image = get_value("interactive", "image")
    if pinned_image and image_name is None:
        image_name = pinned_image

    provider_api = _safe_get_provider(provider)
    config = safe_read_yaml(config_path) if config_path else dict()

    provider_api.pre_start_checks(**kwargs)

    project_name = get_renku_project_name()
    if image_name is None:
        tag = project_context.repository.head.commit.hexsha[:7]
        repo_host = get_image_repository_host()
        image_name = f"{project_name.lower()}:{tag}"
        if repo_host:
            image_name = f"{repo_host}/{image_name}"
    if image_name.lower() != image_name:
        raise errors.SessionStartError(f"Image name '{image_name}' cannot contain upper-case letters.")

    force_build_image = provider_api.force_build_image(**kwargs)

    if not force_build_image and not provider_api.find_image(image_name, config):
        communication.confirm(
            f"The container image '{image_name}' does not exist. Would you like to build it using {provider}?",
            abort=True,
        )
        force_build_image = True

    if force_build_image:
        with communication.busy(msg=f"Building image {image_name}"):
            provider_api.build_image(project_context.dockerfile_path.parent, image_name, config)
        communication.echo(f"Image {image_name} built successfully.")

    # set resource settings
    cpu_limit = cpu_request or get_value("interactive", "cpu_request")

    if cpu_limit is not None:
        try:
            cpu_limit = float(cpu_limit)
        except ValueError:
            raise errors.SessionStartError(f"Invalid value for cpu_request (must be float): {cpu_limit}")

    disk_limit = disk_request or get_value("interactive", "disk_request")
    mem_limit = mem_request or get_value("interactive", "mem_request")
    gpu = gpu_request or get_value("interactive", "gpu_request")

    with communication.busy(msg="Waiting for session to start..."):
        provider_message, warning_message = provider_api.session_start(
            config=config,
            project_name=project_name,
            image_name=image_name,
            cpu_request=cpu_limit,
            mem_request=mem_limit,
            disk_request=disk_limit,
            gpu_request=gpu,
            **kwargs,
        )

    if warning_message:
        communication.warn(warning_message)
    communication.echo(provider_message)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_stop(session_name: Optional[str], stop_all: bool = False, provider: Optional[str] = None):
    """Stop interactive session.

    Args:
        session_name(Optional[str]): Name of the session to open.
        stop_all(bool): Whether to stop all sessions or just the specified one.
        provider(Optional[str]): Name of the session provider to use.
    """

    def stop_sessions(session_provider: ISessionProvider) -> SessionStopStatus:
        try:
            return session_provider.session_stop(
                project_name=project_name, session_name=session_name, stop_all=stop_all
            )
        except errors.RenkulabSessionGetUrlError as e:
            if provider:
                raise
            communication.warn(f"Didn't stop any renkulab sessions: {e}")
            return SessionStopStatus.SUCCESSFUL
        except errors.DockerError as e:
            if provider:
                raise
            communication.warn(f"Didn't stop any docker sessions: {e}")
            return SessionStopStatus.SUCCESSFUL

    session_detail = "all sessions" if stop_all else f"session {session_name}" if session_name else "session"
    project_name = get_renku_project_name()

    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()

    statues = []
    warning_messages = []
    with communication.busy(msg=f"Waiting for {session_detail} to stop..."):
        for session_provider in sorted(providers, key=lambda p: p.priority):
            try:
                status = stop_sessions(session_provider)
            except errors.RenkuException as e:
                warning_messages.append(f"Cannot stop sessions in provider '{session_provider.name}': {e}")
            else:
                statues.append(status)

            # NOTE: The given session name was stopped; don't continue
            if session_name and not stop_all and status == SessionStopStatus.SUCCESSFUL:
                break

    if warning_messages:
        for message in warning_messages:
            communication.warn(message)

    if not statues:
        return
    elif all(s == SessionStopStatus.NO_ACTIVE_SESSION for s in statues):
        raise errors.ParameterError("There are no running sessions.")
    elif session_name and not any(s == SessionStopStatus.SUCCESSFUL for s in statues):
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    elif any(s == SessionStopStatus.FAILED for s in statues):
        raise errors.ParameterError("Cannot stop some sessions")
    elif not session_name and not any(s == SessionStopStatus.SUCCESSFUL for s in statues):
        raise errors.ParameterError("Session name is missing")


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_open(session_name: Optional[str], provider: Optional[str] = None, **kwargs):
    """Open interactive session in the browser.

    Args:
        session_name(Optional[str]): Name of the session to open.
        provider(Optional[str]): Name of the session provider to use.
    """
    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()
    project_name = get_renku_project_name()

    for session_provider in providers:
        if session_provider.session_open(project_name, session_name, **kwargs):
            return

    if session_name:
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    else:
        raise errors.ParameterError("Session name is missing")


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def ssh_setup(existing_key: Optional[Path] = None, force: bool = False):
    """Setup SSH keys for SSH connections to sessions.

    Args:
        existing_key(Path, optional): Existing private key file to use instead of generating new ones.
        force(bool): Whether to prompt before overwriting keys or not
    """

    if not shutil.which("ssh"):
        raise errors.SSHNotFoundError()

    system_config = SystemSSHConfig()

    include_string = f"Include {system_config.renku_ssh_root}/*.conf\n\n"

    if include_string not in system_config.ssh_config.read_text():
        with system_config.ssh_config.open(mode="r+") as f:
            content = f.read()
            f.seek(
                0, 0
            )  # NOTE: We need to add 'Include' before any 'Host' entry, otherwise it is included as part of a host
            f.write(include_string + content)

    if not existing_key and not force and system_config.is_configured:
        communication.confirm(f"Keys already configured for host {system_config.renku_host}. Overwrite?", abort=True)

    if existing_key:
        communication.info("Linking existing keys")
        existing_public_key = existing_key.parent / (existing_key.name + ".pub")

        if not existing_key.exists() or not existing_public_key.exists():
            raise errors.KeyNotFoundError(
                f"Couldn't find private key '{existing_key}' or public key '{existing_public_key}'."
            )

        if system_config.keyfile.exists():
            system_config.keyfile.unlink()
        if system_config.public_keyfile.exists():
            system_config.public_keyfile.unlink()

        os.symlink(existing_key, system_config.keyfile)
        os.symlink(existing_public_key, system_config.public_keyfile)
    else:
        communication.info("Generating keys")
        keys = generate_ssh_keys()
        system_config.keyfile.touch(mode=0o600)
        system_config.public_keyfile.touch(mode=0o644)
        with system_config.keyfile.open(
            "wt",
        ) as f:
            f.write(keys.private_key)

        with system_config.public_keyfile.open("wt") as f:
            f.write(keys.public_key)

    communication.info("Writing SSH config")
    with system_config.jumphost_file.open(mode="wt") as f:
        # NOTE: The * at the end of the jumphost name hides it from VSCode
        content = textwrap.dedent(
            f"""
            Host jumphost-{system_config.renku_host}*
                HostName {system_config.renku_host}
                Port 2022
                User jovyan
            """
        )
        f.write(content)

    communication.warn(
        "This command does not add any public SSH keys to your project. "
        "Keys have to be added manually or by using the 'renku session start' command with the '--ssh' flag."
    )


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_pause(session_name: Optional[str], provider: Optional[str] = None, **kwargs):
    """Pause an interactive session.

    Args:
        session_name(Optional[str]): Name of the session.
        provider(Optional[str]): Name of the session provider to use.
    """

    def pause(session_provider: IHibernatingSessionProvider) -> SessionStopStatus:
        try:
            return session_provider.session_pause(project_name=project_name, session_name=session_name)
        except errors.RenkulabSessionGetUrlError:
            if provider:
                raise
            return SessionStopStatus.FAILED

    project_name = get_renku_project_name()

    if provider:
        session_provider = _safe_get_provider(provider)
        if session_provider is None:
            raise errors.ParameterError(f"Provider '{provider}' not found")
        elif not isinstance(session_provider, IHibernatingSessionProvider):
            raise errors.ParameterError(f"Provider '{provider}' doesn't support pausing sessions")
        providers = [session_provider]
    else:
        providers = get_supported_hibernating_session_providers()

    session_message = f"session {session_name}" if session_name else "session"
    statues = []
    warning_messages = []
    with communication.busy(msg=f"Waiting for {session_message} to pause..."):
        for session_provider in sorted(providers, key=lambda p: p.priority):
            try:
                status = pause(session_provider)  # type: ignore
            except errors.RenkuException as e:
                warning_messages.append(f"Cannot pause sessions in provider '{session_provider.name}': {e}")
            else:
                statues.append(status)

            # NOTE: The given session name was stopped; don't continue
            if session_name and status == SessionStopStatus.SUCCESSFUL:
                break

    if warning_messages:
        for message in warning_messages:
            communication.warn(message)

    if not statues:
        return
    elif all(s == SessionStopStatus.NO_ACTIVE_SESSION for s in statues):
        raise errors.ParameterError("There are no running sessions.")
    elif session_name and not any(s == SessionStopStatus.SUCCESSFUL for s in statues):
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    elif not session_name and not any(s == SessionStopStatus.SUCCESSFUL for s in statues):
        raise errors.ParameterError("Session name is missing")


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def session_resume(session_name: Optional[str], provider: Optional[str] = None, **kwargs):
    """Resume a paused session.

    Args:
        session_name(Optional[str]): Name of the session.
        provider(Optional[str]): Name of the session provider to use.
    """
    project_name = get_renku_project_name()

    if provider:
        session_provider = _safe_get_provider(provider)
        if session_provider is None:
            raise errors.ParameterError(f"Provider '{provider}' not found")
        elif not isinstance(session_provider, IHibernatingSessionProvider):
            raise errors.ParameterError(f"Provider '{provider}' doesn't support pausing/resuming sessions")
        providers = [session_provider]
    else:
        providers = get_supported_hibernating_session_providers()

    session_message = f"session {session_name}" if session_name else "session"
    with communication.busy(msg=f"Waiting for {session_message} to resume..."):
        for session_provider in providers:
            if session_provider.session_resume(project_name, session_name, **kwargs):  # type: ignore
                return

    if session_name:
        raise errors.ParameterError(f"Could not find '{session_name}' among the sessions.")
    else:
        raise errors.ParameterError("Session name is missing")
