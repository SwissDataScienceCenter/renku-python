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
"""SSH utility functions."""

import textwrap
import urllib.parse
from pathlib import Path
from typing import NamedTuple, Optional, cast

from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from renku.core import errors
from renku.core.session.utils import get_renku_url
from renku.core.util import communication
from renku.domain_model.project_context import project_context


class SSHKeyPair(NamedTuple):
    """A public/private key pair for SSH."""

    private_key: str
    public_key: str


def generate_ssh_keys() -> SSHKeyPair:
    """Generate an SSH key pair.

    Returns:
        Private Public key pair.
    """
    key = Ed25519PrivateKey.generate()

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.OpenSSH,
        crypto_serialization.NoEncryption(),
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    )

    return SSHKeyPair(private_key.decode("utf-8"), public_key.decode("utf-8"))


class SystemSSHConfig:
    """Class to manage system SSH config."""

    def __init__(self) -> None:
        """Initialize class and calculate paths."""
        self.ssh_root: Path = Path.home() / ".ssh"
        self.ssh_config: Path = self.ssh_root / "config"
        self.renku_ssh_root: Path = self.ssh_root / "renku"

        self.renku_ssh_root.mkdir(mode=0o700, exist_ok=True, parents=True)
        self.ssh_config.touch(mode=0o600, exist_ok=True)

        self.renku_host: Optional[str] = cast(Optional[str], urllib.parse.urlparse(get_renku_url()).hostname)

        if not self.renku_host:
            raise errors.AuthenticationError(
                "Please use `renku login` to log in to the remote deployment before setting up ssh."
            )

        self.jumphost_file = self.renku_ssh_root / f"99-{self.renku_host}-jumphost.conf"
        self.keyfile = self.renku_ssh_root / f"{self.renku_host}-key"
        self.public_keyfile = self.renku_ssh_root / f"{self.renku_host}-key.pub"

    @property
    def is_configured(self) -> bool:
        """Check if the system is already configured correctly."""
        return self.jumphost_file.exists() and self.keyfile.exists() and self.public_keyfile.exists()

    @property
    def public_key_string(self) -> Optional[str]:
        """Get the public key string, ready for authorized_keys."""
        try:
            key = self.public_keyfile.read_text()
            key = f"\n{key} {project_context.repository.get_user().name}"
            return key
        except FileNotFoundError:
            return None

    def is_session_configured(self, session_name: str) -> bool:
        """Check if a session is configured for SSH.

        Args:
            session_name(str): The name of the session.
        """
        if not project_context.ssh_authorized_keys_path.exists():
            return False

        session_commit = session_name.rsplit("-", 1)[-1]

        try:
            project_context.repository.get_commit(session_commit)
        except errors.GitCommitNotFoundError:
            return False

        try:
            authorized_keys = project_context.repository.get_content(
                project_context.ssh_authorized_keys_path, revision=session_commit
            )
        except errors.FileNotFound:
            return False

        if self.public_key_string and self.public_key_string in authorized_keys:
            return True
        return False

    def session_config_path(self, project_name: str, session_name: str) -> Path:
        """Get path to a session config.

        Args:
            project_name(str): The name of the project, without the owner name.
            session_name(str): The name of the session to setup a connection to.
        Returns:
            The path to the SSH connection file.
        """
        return self.renku_ssh_root / f"00-{project_name}-{session_name}.conf"

    def setup_session_keys(self) -> bool:
        """Add a users key to a project."""
        project_context.ssh_authorized_keys_path.parent.mkdir(parents=True, exist_ok=True)
        project_context.ssh_authorized_keys_path.touch(mode=0o600, exist_ok=True)

        if not self.public_key_string:
            raise errors.SSHNotSetupError()

        if self.public_key_string in project_context.ssh_authorized_keys_path.read_text():
            return False

        communication.info("Adding SSH public key to project.")
        with project_context.ssh_authorized_keys_path.open("at") as f:
            f.writelines(self.public_key_string)

        project_context.repository.add(project_context.ssh_authorized_keys_path)
        project_context.repository.commit("Add SSH public key.")
        communication.info(
            "Added public key. Changes need to be pushed and remote image built for changes to take effect."
        )
        return True

    def setup_session_config(self, project_name: str, session_name: str) -> str:
        """Setup local SSH config for connecting to a session.

        Args:
            project_name(str): The name of the project, without the owner name.
            session_name(str): The name of the session to setup a connection to.
        Returns:
            The name of the created SSH host config.
        """
        path = self.session_config_path(project_name, session_name)
        path.touch(mode=0o600, exist_ok=True)

        config_content = textwrap.dedent(
            f"""
            Host {session_name}
                HostName {session_name}
                RemoteCommand cd work/{project_name}/ || true && exec $SHELL --login
                RequestTTY yes
                ServerAliveInterval 15
                ServerAliveCountMax 3
                ProxyJump  jumphost-{self.renku_host}
                IdentityFile {self.keyfile}
                IdentityFile ~/.ssh/id_rsa
                IdentityFile ~/.ssh/id_ecdsa
                IdentityFile ~/.ssh/id_ecdsa_sk
                IdentityFile ~/.ssh/id_ed25519
                IdentityFile ~/.ssh/id_ed25519_sk
                IdentityFile ~/.ssh/id_dsa
                User jovyan
                StrictHostKeyChecking no
            """
        )

        path.write_text(config_content)

        return session_name
