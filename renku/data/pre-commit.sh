#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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

######################################
# RENKU HOOK. DO NOT REMOVE OR MODIFY.
######################################

# Find all modified or added files, and do nothing if there aren't any.
export RENKU_DISABLE_VERSION_CHECK=true
IFS=$'\n' read -r -d '' -a MODIFIED_FILES \
  <<< "$(git diff --name-only --cached --diff-filter=M)"
IFS=$'\n' read -r -d '' -a ADDED_FILES \
  <<< "$(git diff --name-only --cached --diff-filter=A)"

if [ ${#MODIFIED_FILES[@]} -ne 0 ] || [ ${#ADDED_FILES[@]} -ne 0 ]; then
  # Verify that renku is installed; if not, warn and exit.
  if [ -z "$(command -v renku)" ]; then
    echo 'renku not on path; can not format. Please install renku:'
    # TODO add system detection and recommend brew for macOS.
    echo '    pip install renku'
    exit 2
  fi
fi

if [ ${#MODIFIED_FILES[@]} -ne 0 ] ; then
  MODIFIED_OUTPUTS=$(renku show outputs "${MODIFIED_FILES[@]}")
  if [ "$MODIFIED_OUTPUTS" ]; then
    echo 'You are trying to update generated files.'
    echo
    echo 'Modified files:'
    for file in "${MODIFIED_OUTPUTS[@]}"; do
      echo "$file"
    done
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
  IMMUTABLE_TEMPLATE_FILES=$(renku check-immutable-template-files "${MODIFIED_FILES[@]}")
  if [ "$IMMUTABLE_TEMPLATE_FILES" ]; then
    echo 'You are trying to update files marked as immutable in your project template.'
    echo 'This would prevent the project from being updated with new versions of the template in the future.'
    echo
    echo 'Immutable files:'
    for file in "${MODIFIED_OUTPUTS[@]}"; do
      echo "$file"
    done
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
fi

if [ ${#ADDED_FILES[@]} -ne 0 ]; then
  UNTRACKED_PATHS=$(renku storage check-lfs-hook "${ADDED_FILES[@]}")
  if [ "$UNTRACKED_PATHS" ]; then
    echo 'You are trying to commit large files to Git instead of Git-LFS.'
    echo
    echo 'Large files:'
    for file in "${UNTRACKED_PATHS[@]}"; do
      echo "$file"
    done
    echo
    echo 'To track these files in Git LFS use "git lfs track <FILENAMES>".'
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
fi

######################################
# END RENKU HOOK.
######################################
