# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Custom hooks for Jupyter notebook."""

from __future__ import absolute_import

import os

import nbformat
from notebook import notebookapp  # needed for translation setup
from notebook.services.contents.largefilemanager import LargeFileManager

import renga


class RengaFileManager(LargeFileManager):
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RengaFileManager, self).__init__(*args, **kwargs)
        self._renga_client = renga.from_env()

    def _save_notebook(self, os_path, nb):
        """Save a notebook to the storage service."""
        result = super(RengaFileManager, self)._save_notebook(os_path, nb)

        bucket_id = os.environ.get('RENGA_BUCKET_ID')
        file_id = os.environ.get('RENGA_FILE_ID')

        with self._renga_client.buckets[int(bucket_id)].files[int(
                file_id)].open('w') as fp:
            fp.write(nbformat.writes(nb, version=nbformat.NO_CONVERT))

        return result
