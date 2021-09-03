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
"""Test various utilities."""

import os

from renku.core.errors import ParameterError
from renku.core.utils.scm import shorten_message
from renku.core.utils.urls import get_host
from tests.utils import raises


def test_hostname():
    """Test host is set correctly in a different Renku domain."""
    renku_domain = os.environ.get("RENKU_DOMAIN")
    try:
        os.environ["RENKU_DOMAIN"] = "alternative-domain"

        assert "alternative-domain" == get_host(None)
    finally:
        if renku_domain:
            os.environ["RENKU_DOMAIN"] = renku_domain
        else:
            del os.environ["RENKU_DOMAIN"]


def test_shorten_message():
    """Test message is shorten correctly."""

    short_message = "Lorem ipsum dolor sit amet"
    long_message = (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt "
        "ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco "
        "laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in "
        "voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat "
        "non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
    )
    max_line = 150

    # The message is modified only when it's longer than the max line length
    assert shorten_message(short_message, max_line) == short_message
    assert shorten_message(long_message, max_line) != long_message
    assert "\n" in shorten_message(long_message, max_line)
    assert shorten_message(long_message, 0) == long_message
    assert shorten_message(long_message, len(long_message)) == long_message
    assert len(shorten_message(long_message, max_line)) > len(long_message)

    # The message is trimmed when it's longer than the max body length
    assert len(shorten_message(long_message, 0, body_length=max_line)) < len(long_message)
    assert len(shorten_message(short_message, 0, body_length=max_line)) == len(short_message)
    assert len(shorten_message(long_message, 0, body_length=0)) == len(long_message)
    assert len(shorten_message(long_message, 0, body_length=len(long_message))) == len(long_message)

    # The the message is wrapped correctly
    assert len(shorten_message(long_message, max_line, body_length=max_line * 2)) < len(long_message)
    assert len(shorten_message(short_message, max_line, body_length=max_line * 2)) == len(short_message)
    assert shorten_message(long_message, max_line, body_length=max_line * 2).count("\n") == 1

    # The message is not distored
    restored_message = shorten_message(long_message, max_line).replace("\n\t", " ")
    assert restored_message == long_message

    restored_message = shorten_message(long_message, max_line, body_length=max_line * 2).replace("\n\t", " ")
    restored_message = restored_message[: max_line * 2 - 3]
    assert long_message.startswith(restored_message)
    assert not restored_message.startswith(long_message)

    # Negative lengths are not accepted
    with raises(ParameterError):
        shorten_message(short_message, -1)
    with raises(ParameterError):
        shorten_message(short_message, max_line, -1)
