# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""For backwards compatibility, ``renku.core.metadata`` was moved to ``renku.infrastructure``."""

import warnings

import renku.infrastructure as infrastructure

warnings.warn("The renku.core.metadata module has moved to renku.infrastructure and is deprecated", DeprecationWarning)

globals().update({k: getattr(infrastructure, k) for k in infrastructure.__dict__.keys() if not k.startswith("_")})
