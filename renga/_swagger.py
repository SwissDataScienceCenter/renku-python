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
"""Combine Swagger specifications."""


def merge(*specs):
    """Merge paths and definitions from given specifications."""
    composed = {'paths': {}, 'definitions': {}}

    for definition in specs:
        # combines paths
        for key, path in definition['paths'].items():
            composed['paths'][definition['basePath'] + key] = path

        # combines definitions
        for key, defs in definition['definitions'].items():
            assert key not in composed['definitions']
            composed['definitions'][key] = defs

    return composed
