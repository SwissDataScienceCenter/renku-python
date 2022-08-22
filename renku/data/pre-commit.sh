#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
  IFS=$'\n' read -r -d '' -a IMMUTABLE_TEMPLATE_FILES \
    <<< "$(renku check-immutable-template-files "${MODIFIED_FILES[@]}")"
  if [ ${#IMMUTABLE_TEMPLATE_FILES[@]} -ne 0 ]; then
    echo 'You are trying to update files marked as immutable in your project template.'
    echo 'This would prevent the project from being updated with new versions of the template in the future.'
    echo
    echo 'Immutable files:'
    for file in "${IMMUTABLE_TEMPLATE_FILES[@]}"; do
      echo "$file"
    done
    echo
    echo 'To commit anyway, use "git commit --no-verify".'
    exit 1
  fi
fi

if [ ${#ADDED_FILES[@]} -ne 0 ]; then
  IFS=$'\n' read -r -d '' -a UNTRACKED_PATHS \
    <<< "$(renku storage check-lfs-hook "${ADDED_FILES[@]}")"
  if [ ${#UNTRACKED_PATHS[@]} -ne 0 ]; then
    echo 'You are trying to commit large files to Git instead of Git-LFS.'
    echo
    AUTOCOMMIT_LFS=${AUTOCOMMIT_LFS:=$(renku config show autocommit_lfs)}
    if [ "$AUTOCOMMIT_LFS" = "true" ]; then
      echo 'Adding files to LFS:'
      for file in "${UNTRACKED_PATHS[@]}"; do
        echo "$file"
      done
      echo
      git lfs track -- "${UNTRACKED_PATHS[@]}"
      git add .gitattributes
      git add -- "${UNTRACKED_PATHS[@]}"
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

if [ ${#MODIFIED_FILES[@]} -ne 0 ] || [ ${#ADDED_FILES[@]} -ne 0 ]; then
  CHECK_DATADIR=${CHECK_DATADIR:=$(renku config show check_datadir_files)}
  if [ "$CHECK_DATADIR" = "true" ]; then
    ALL_FILES=( "${MODIFIED_FILES[@]}" "${ADDED_FILES[@]}")
    ARGS=()
    for file in "${ALL_FILES[@]}"; do
      ARGS+=("-I" "$file")
    done
    IFS=$'\n' read -r -d '' -a DATASET_FILES \
      <<< "$(renku dataset update -n --no-external --no-remote -c --plain "${ARGS[@]}")"

    if [ ${#DATASET_FILES[@]} -ne 0 ]; then
      echo "Files in datasets data directory that aren't up to date:"
      echo
      for entry in "${DATASET_FILES[@]}"; do
        read -r change path dataset <<< "$entry"

        if [ "$change" = "f" ]; then
          echo "$path ($dataset) modified"
        elif [ "$change" = "r" ]; then
          echo "$path ($dataset) removed"
        fi
      done
      echo
      echo 'Run "renku dataset update -c --all --no-remote --no-external" to update the datasets.'
      echo
      echo 'To disable this check, run "renku config set check_datadir_files false".'
      exit 1
    fi
  fi
fi

######################################
# END RENKU HOOK.
######################################
