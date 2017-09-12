# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Renga exceptions."""

import requests


class RengaException(Exception):
    """A base class for all Renga related exception.

    You can catch all errors raised by Renga SDK by using
    ``except RengaException:``.
    """


class APIError(requests.exceptions.HTTPError, RengaException):
    """Catch HTTP errors from API calls."""

    @classmethod
    def from_http_exception(cls, e):
        """Create ``APIError`` from ``requests.exception.HTTPError``."""
        assert isinstance(e, requests.exceptions.HTTPError)
        response = e.response
        try:
            message = response.json()['message']
        except ValueError:
            message = response.content.strip()

        raise cls(message)


class InvalidFileOperation(RengaException):
    """Raise when trying to perfrom invalid file operation."""


class NotFound(APIError):
    """Raise when an API object is not found."""
