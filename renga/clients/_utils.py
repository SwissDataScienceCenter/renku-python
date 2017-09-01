# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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
"""Client related helper functions."""


def return_response(response, ok_code, return_json=False):
    """Return boolean or response json if ok_code in response."""
    if response.status_code == ok_code:
        if return_json:
            return response.json()
        else:
            return True
    else:
        raise RuntimeError(
            'Request failed ({})'.format(response.status_code))
