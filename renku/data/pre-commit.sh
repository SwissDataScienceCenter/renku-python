#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

# Find all modified files, and exit early if there aren't any.
MODIFIED_FILES=$(git diff --name-only --cached --diff-filter=M)
if [ ! "$MODIFIED_FILES" ]; then
  exit 0
fi

# Verify that renku is installed; if not, warn and exit.
if [ -z "$(command -v renku)" ]; then
  echo 'renku not on path; can not format. Please install renku:'
  # TODO add system detection and recommend brew for macOS.
  echo '    pip install renku'
  exit 2
fi

MODIFIED_OUTPUTS=$(renku show outputs "${MODIFIED_FILES[@]}")
if [ "$MODIFIED_OUTPUTS" ]; then
  echo 'You are trying to update generated files.'
  echo
  echo 'Modified files:'
  for file in "${MODIFIED_OUTPUTS[@]}"; do
    echo "  $file"
  done
  echo
  echo 'To commit anyway, use "git commit --no-verify".'
  exit 1
else
  exit 0
fi
