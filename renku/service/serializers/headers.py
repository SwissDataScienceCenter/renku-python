# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service headers serializers."""
import base64
import binascii
import os

import jwt
from marshmallow import Schema, ValidationError, fields, post_load, pre_load
from werkzeug.utils import secure_filename

JWT_TOKEN_SECRET = os.getenv("RENKU_JWT_TOKEN_SECRET", "bW9menZ3cnh6cWpkcHVuZ3F5aWJycmJn")


def decode_b64(value):
    """Decode base64 values or return raw value."""
    try:
        decoded = base64.b64decode(value, validate=True)
        return decoded.decode("utf-8")
    except binascii.Error:
        return value


def encode_b64(value):
    """Encode value to base64."""
    if isinstance(value, str):
        value = bytes(value, "utf-8")

    return base64.b64encode(value).decode("utf-8")


class UserIdentityToken(Schema):
    """User identity token schema."""

    jti = fields.String()
    exp = fields.Integer()
    nbf = fields.Integer()
    iat = fields.Integer()
    iss = fields.String()
    aud = fields.List(fields.String())
    sub = fields.String()
    typ = fields.String()
    azp = fields.String()
    nonce = fields.String()
    auth_time = fields.Integer()
    session_state = fields.String()
    acr = fields.String()
    email_verified = fields.Boolean()
    preferred_username = fields.String()
    given_name = fields.String()
    family_name = fields.String()

    email = fields.String(required=True)
    name = fields.String(required=True)
    user_id = fields.String()  # INFO: Generated post load.

    @post_load
    def set_user_id(self, data, **kwargs):
        """Sets users id."""
        data["user_id"] = encode_b64(secure_filename(data["sub"]))
        return data


class UserIdentityHeaders(Schema):
    """User identity schema."""

    user_token = fields.String(required=True, data_key="renku-user")
    auth_token = fields.String(required=True, data_key="authorization")

    @staticmethod
    def decode_token(token):
        """Extract authorization token."""
        components = token.split(" ")

        rfc_compliant = token.lower().startswith("bearer")
        rfc_compliant &= len(components) == 2

        if not rfc_compliant:
            raise ValidationError("authorization value contains invalid value")

        return components[-1]

    @staticmethod
    def decode_user(data):
        """Extract renku user from a JWT."""
        decoded = jwt.decode(data, JWT_TOKEN_SECRET, algorithms=["HS256"], audience="renku",)
        return UserIdentityToken().load(decoded)

    @staticmethod
    def reset_old_headers(data):
        """Process old version of old headers."""
        # TODO: This should be removed once support for them is phased out.
        if "renku-user-id" in data:
            data.pop("renku-user-id")

        if "renku-user-fullname" in data and "renku-user-email" in data:
            renku_user = {
                "aud": ["renku"],
                "name": decode_b64(data.pop("renku-user-fullname")),
                "email": decode_b64(data.pop("renku-user-email")),
            }
            renku_user["sub"] = renku_user["email"]
            data["renku-user"] = jwt.encode(renku_user, JWT_TOKEN_SECRET, algorithm="HS256").decode("utf-8")

        return data

    @pre_load
    def set_fields(self, data, **kwargs):
        """Set fields for serialization."""
        # NOTE: We don't process headers which are not meant for determining identity.
        # TODO: Remove old headers support once support for them is phased out.
        old_keys = ["renku-user-id", "renku-user-fullname", "renku-user-email"]
        expected_keys = old_keys + [field.data_key for field in self.fields.values()]

        data = {key.lower(): value for key, value in data.items() if key.lower() in expected_keys}
        data = self.reset_old_headers(data)

        return data

    @post_load
    def set_user(self, data, **kwargs):
        """Extract user object from a JWT."""
        user = self.decode_user(data["user_token"])
        return {
            "fullname": user.pop("name"),
            "email": user.pop("email"),
            "user_id": user.pop("user_id"),
            "token": self.decode_token(data["auth_token"]),
        }
