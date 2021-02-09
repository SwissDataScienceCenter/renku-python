# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
import jwt

from renku.service.serializers.headers import JWT_TOKEN_SECRET, RequiredIdentityHeaders


def test_header_serializer(identity_headers):
    """Check expected serialization for service headers."""
    decoded = jwt.decode(identity_headers["Renku-User"], JWT_TOKEN_SECRET, algorithms=["HS256"], audience="renku")
    assert {
        "jti",
        "exp",
        "nbf",
        "iat",
        "iss",
        "aud",
        "sub",
        "typ",
        "azp",
        "nonce",
        "auth_time",
        "session_state",
        "acr",
        "email_verified",
        "preferred_username",
        "given_name",
        "family_name",
        "name",
        "email",
    } == set(decoded.keys())

    user_identity = RequiredIdentityHeaders().load(identity_headers)
    assert {"fullname", "email", "user_id", "token"} == set(user_identity.keys())


def test_old_headers():
    """Test old version of headers."""
    old_headers = {
        "Content-Type": "application/json",
        "Renku-User-Id": "user",
        "Renku-User-FullName": "full name",
        "Renku-User-Email": "renku@sdsc.ethz.ch",
        "Authorization": "Bearer None",
    }

    user_identity = RequiredIdentityHeaders().load(old_headers)
    assert {"fullname", "email", "user_id", "token"} == set(user_identity.keys())
