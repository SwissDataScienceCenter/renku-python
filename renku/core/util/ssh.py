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
"""SSH utility functions."""

import urllib.parse
from pathlib import Path
from typing import NamedTuple

from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from renku.core import errors
from renku.core.session.utils import get_renku_url

SSHKeyPair = NamedTuple("SSHKeyPair", [("private_key", str), ("public_key", str)])


def generate_ssh_keys() -> SSHKeyPair:
    """Generate an SSH keypair.

    Returns:
        Private Public key pair.
    """
    key = rsa.generate_private_key(backend=crypto_default_backend(), public_exponent=65537, key_size=4096)

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM, crypto_serialization.PrivateFormat.PKCS8, crypto_serialization.NoEncryption()
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    )

    return SSHKeyPair(private_key.decode("utf-8"), public_key.decode("utf-8"))


class SystemSSHConfig:
    """Class to manage system SSH config."""

    def __init__(self) -> None:
        """Initialize class ans calculate paths."""
        self.ssh_root: Path = Path.home() / ".ssh"
        self.ssh_config: Path = self.ssh_root / "config"
        self.renku_ssh_root: Path = self.ssh_root / "renku"

        self.renku_ssh_root.mkdir(exist_ok=True, parents=True)
        self.ssh_config.touch(mode=0o644, exist_ok=True)

        self.renku_host: str = str(urllib.parse.urlparse(get_renku_url()).hostname)

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

    def session_config_path(self, project_name: str, session_name: str) -> Path:
        """Get path to a session config.

        Args:
            project_name(str): The name of the project, potentially with the owner name.
            session_name(str): The name of the session to setup a connection to.
        Returns:
            The path to the SSH connection file.
        """
        return self.renku_ssh_root / f"00-{project_name}-{session_name}.conf"

    def connection_name(self, project_name: str, session_name: str) -> str:
        """Get the connection name for an ssh connection.

        Args:
            project_name(str): The name of the project, potentially with the owner name.
            session_name(str): The name of the session to setup a connection to.
        Returns:
            The name of the SSH connection.
        """
        return f"{self.renku_host}-{project_name}-{session_name}"

    def setup_session_config(self, project_name: str, session_name: str) -> str:
        """Setup local SSH config for connecting to a session.

        Args:
            project_name(str): The name of the project, potentially with the owner name.
            session_name(str): The name of the session to setup a connection to.
        Returns:
            The name of the created SSH host config.
        """
        connection_name = self.connection_name(project_name, session_name)

        path = self.session_config_path(project_name, session_name)
        path.touch(mode=0o644, exist_ok=True)

        path.write_text(
            f"""
Host {connection_name}
    HostName {session_name}
"""
        )

        return connection_name
