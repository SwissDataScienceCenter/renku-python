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
"""Renku service headers serializers."""
import base64
import binascii
import os
from typing import cast

import jwt
from flask import app
from marshmallow import Schema, ValidationError, fields, post_load
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


class RenkuHeaders:
    """Renku headers support."""

    @staticmethod
    def decode_token(token):
        """Extract the Gitlab access token form a bearer authorization header value."""
        components = token.split(" ")

        rfc_compliant = token.lower().startswith("bearer")
        rfc_compliant &= len(components) == 2

        if not rfc_compliant:
            raise ValidationError("authorization value contains invalid value")

        return components[-1]

    @staticmethod
    def decode_user(data):
        """Extract renku user from the Keycloak ID token which is a JWT."""
        try:
            jwk = cast(jwt.PyJWKClient, app.config["KEYCLOAK_JWK_CLIENT"])
            key = jwk.get_signing_key_from_jwt(data)
            decoded = jwt.decode(data, key=key, algorithms=["RS256"], audience="renku")
        except jwt.PyJWTError:
            # NOTE: older tokens used to be signed with HS256 so use this as a backup if the validation with RS256
            # above fails. We used to need HS256 because a step that is now removed was generating an ID token and
            # signing it from data passed in individual header fields.
            decoded = jwt.decode(data, JWT_TOKEN_SECRET, algorithms=["HS256"], audience="renku")
        return UserIdentityToken().load(decoded)


class IdentityHeaders(Schema):
    """User identity schema."""

    @post_load
    def set_user(self, data, **kwargs):
        """Extract user object from a JWT."""
        result = {}

        if "auth_token" in data:
            result["token"] = RenkuHeaders.decode_token(data["auth_token"])

        if data and "user_token" in data:
            user = RenkuHeaders.decode_user(data["user_token"])
            result["fullname"] = user.pop("name")
            result["email"] = user.pop("email")
            result["user_id"] = user.pop("user_id")

        return result


class RequiredIdentityHeaders(IdentityHeaders):
    """Identity schema for required headers."""

    user_token = fields.String(required=True, data_key="renku-user")  # Keycloak ID token
    auth_token = fields.String(required=True, data_key="authorization")  # Gitlab access token


class OptionalIdentityHeaders(IdentityHeaders):
    """Identity schema for optional headers."""

    user_token = fields.String(data_key="renku-user")  # Keycloak ID token
    auth_token = fields.String(data_key="authorization")  # Gitlab access token
