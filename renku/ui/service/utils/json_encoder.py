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
"""Renku service response json encoder."""

import orjson
from flask.json.provider import JSONProvider


class SvcJSONProvider(JSONProvider):
    """Custom JSON provider that supports iso datetimes."""

    def dumps(self, obj, *, option=None, **kwargs):
        """Dump JSON object to string."""
        return orjson.dumps(obj, option=option)

    def loads(self, s, **kwargs):
        """Load JSON object from string."""
        return orjson.loads(s)
