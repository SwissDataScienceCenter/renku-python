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
from marshmallow import Schema, ValidationError, fields, pre_load
from werkzeug.utils import secure_filename


class UserIdentityHeaders(Schema):
    """User identity schema."""

    user_id = fields.String(required=True, data_key='renku-user-id')
    fullname = fields.String(data_key='renku-user-fullname')
    email = fields.String(data_key='renku-user-email')
    token = fields.String(data_key='authorization')

    def extract_token(self, data):
        """Extract token."""
        value = data.get('authorization', '')
        components = value.split(' ')

        rfc_compliant = value.lower().startswith('bearer')
        rfc_compliant &= len(components) == 2

        if not rfc_compliant:
            raise ValidationError('authorization value contains invalid value')

        return components[-1]

    @pre_load()
    def set_fields(self, data, **kwargs):
        """Set fields for serialization."""
        expected_keys = [field.data_key for field in self.fields.values()]

        data = {
            key.lower(): value
            for key, value in data.items() if key.lower() in expected_keys
        }

        if {'renku-user-id', 'authorization'}.issubset(set(data.keys())):
            data['renku-user-id'] = secure_filename(data['renku-user-id'])
            data['authorization'] = self.extract_token(data)

        return data
