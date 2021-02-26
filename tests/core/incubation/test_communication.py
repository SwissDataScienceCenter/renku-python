# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Test command builder."""

import threading

import pytest

from renku.cli.utils.callback import ClickCallback
from renku.core.incubation.command import Command
from renku.core.utils import communication
from renku.service.utils.callback import ServiceCallback


def test_communicator_is_unsubscribed(tmp_path):
    """Test communicator is unsubscribed when command is executed."""
    communicator = ServiceCallback()

    command = Command().command(lambda _: communication.echo("Hello world!"))
    command.with_communicator(communicator).build().execute()

    assert ["Hello world!"] == communicator.messages

    communication.echo("More messages.")

    assert ["Hello world!"] == communicator.messages


def test_multi_communicators(tmp_path):
    """Test subscribing multiple communicators."""
    communicator_1 = ServiceCallback()
    communicator_2 = ServiceCallback()

    command = Command().command(lambda _: communication.echo("Hello world!"))
    command.with_communicator(communicator_1).with_communicator(communicator_2).build().execute()

    assert ["Hello world!"] == communicator_1.messages
    assert ["Hello world!"] == communicator_2.messages


def test_multi_threaded_communication(tmp_path):
    """Test communication with multi-threading."""

    def thread_function(name, communicator):
        command = Command().command(lambda _: communication.echo(f"Hello world from {name}!"))
        command.with_communicator(communicator).build().execute()

    communicator_1 = ServiceCallback()
    thread_1 = threading.Thread(target=thread_function, args=("thread-1", communicator_1))

    communicator_2 = ServiceCallback()
    thread_2 = threading.Thread(target=thread_function, args=("thread-2", communicator_2))

    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

    assert ["Hello world from thread-1!"] == communicator_1.messages
    assert ["Hello world from thread-2!"] == communicator_2.messages


def test_click_callback(capsys):
    """Test communication with ClickCallback."""
    communicator = ClickCallback()
    command = Command().command(lambda _: communication.echo("Hello world!"))
    command.with_communicator(communicator).build().execute()

    assert "Hello world!" in capsys.readouterr().out


@pytest.mark.serial
def test_no_callback(capsys):
    """Test communication does not print anything without a callback."""
    command = Command().command(lambda _: communication.echo("Hello world!"))
    command.build().execute()

    assert "Hello world!" not in capsys.readouterr().out
