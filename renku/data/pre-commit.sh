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
  EXIT_CODE=$?
  if [ $EXIT_CODE -eq 3 ]; then
    echo "Cannot verify validity of the commit: Project metadata is outdated."
    echo "Run 'renku migrate' command to fix the issue."
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
  if [ $EXIT_CODE -eq 4 ]; then
    echo "Cannot verify validity of the commit: Project was created with a newer version of Renku."
    echo "Upgrade Renku to the latest version."
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
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
    AUTOCOMMIT_LFS=${AUTOCOMMIT_LFS:=$(renku config show autocommit_lfs)}
    if [ "$AUTOCOMMIT_LFS" = "true" ]; then
      echo 'Adding files to LFS:'
      for file in "${UNTRACKED_PATHS[@]}"; do
        echo "$file"
      done
      echo
      saveIFS=$IFS
      IFS=$' '
      files=${UNTRACKED_PATHS[*]}
      git lfs track -- "$files"
      git add .gitattributes
      IFS=$saveIFS
    else
      echo 'Large files:'
      for file in "${UNTRACKED_PATHS[@]}"; do
        echo "$file"
      done
      echo
      echo 'To track these files in Git LFS use "git lfs track <FILENAMES>".'
      echo
      echo 'To autocommit files to lfs, use "renku config set autocommit_lfs true".'
      echo
      echo 'To commit anyway, use "git commit --no-verify".'
      exit 1
    fi
  fi
fi

######################################
# END RENKU HOOK.
######################################
