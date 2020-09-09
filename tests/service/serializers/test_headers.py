# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
"""Tests for service header serializers."""

from renku.service.serializers.headers import UserIdentityHeaders


def test_header_serializer(identity_headers):
    """Check expected serialization for service headers."""
    user_identity = UserIdentityHeaders().load(identity_headers)

    assert {
        "sub",
        "preferred_username",
        "given_name",
        "auth_time",
        "acr",
        "email_verified",
        "typ",
        "azp",
        "exp",
        "nbf",
        "aud",
        "nonce",
        "iss",
        "jti",
        "family_name",
        "email",
        "session_state",
        "iat",
        "fullname",
        "user_id",
        "token",
    } == set(user_identity.keys())
