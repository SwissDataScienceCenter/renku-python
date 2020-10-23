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
"""Renku service cache upload files controller."""
import os
import shutil
from pathlib import Path

import patoolib
from patoolib.util import PatoolError

from renku.core.errors import RenkuException
from renku.service.config import CACHE_UPLOADS_PATH, SUPPORTED_ARCHIVES
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.serializers.cache import FileUploadRequest, FileUploadResponseRPC, extract_file
from renku.service.views import result_response


class UploadFilesCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for upload files endpoint."""

    REQUEST_SERIALIZER = FileUploadRequest()
    RESPONSE_SERIALIZER = FileUploadResponseRPC()

    def __init__(self, cache, user_data, flask_request):
        """Construct controller."""
        self.file = extract_file(flask_request)

        self.response_builder = {
            "file_name": self.file.filename,
            "content_type": self.file.content_type,
            "is_archive": self.file.content_type in SUPPORTED_ARCHIVES,
        }
        self.response_builder.update(UploadFilesCtrl.REQUEST_SERIALIZER.load(flask_request.args))

        super(UploadFilesCtrl, self).__init__(cache, user_data, {})

    @property
    def context(self):
        """Controller operation context."""
        return self.response_builder

    def process_file(self):
        """Process uploaded file."""
        user_cache_dir = CACHE_UPLOADS_PATH / self.user.user_id
        user_cache_dir.mkdir(exist_ok=True)

        file_path = user_cache_dir / self.file.filename
        if file_path.exists():
            if self.response_builder.get("override_existing", False):
                file_path.unlink()
            else:
                raise RenkuException("file exists")

        self.file.save(str(file_path))

        files = []
        if self.response_builder["unpack_archive"] and self.response_builder["is_archive"]:
            unpack_dir = "{0}.unpacked".format(file_path.name)
            temp_dir = file_path.parent / Path(unpack_dir)
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            temp_dir.mkdir(exist_ok=True)

            try:
                patoolib.extract_archive(str(file_path), outdir=str(temp_dir))
            except PatoolError:
                return RenkuException("unable to unpack archive")

            for file_ in temp_dir.glob("**/*"):
                relative_path = file_.relative_to(user_cache_dir)

                file_obj = {
                    "file_name": file_.name,
                    "file_size": os.stat(file_).st_size,
                    "relative_path": str(relative_path),
                    "is_dir": file_.is_dir(),
                }

                files.append(file_obj)

        else:
            relative_path = file_path.relative_to(CACHE_UPLOADS_PATH / self.user.user_id)

            self.response_builder["file_size"] = os.stat(file_path).st_size
            self.response_builder["relative_path"] = str(relative_path)
            self.response_builder["is_dir"] = file_path.is_dir()

            files.append(self.response_builder)

        files = self.cache.set_files(self.user, files)
        return {"files": files}

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(UploadFilesCtrl.RESPONSE_SERIALIZER, self.process_file())
