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
from renku.core.util.file_size import bytes_to_unit
from renku.ui.service.config import CACHE_UPLOADS_PATH, MAX_CONTENT_LENGTH, SUPPORTED_ARCHIVES
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.errors import IntermittentFileExistsError, UserUploadTooLargeError
from renku.ui.service.serializers.cache import FileUploadRequest, FileUploadResponseRPC, extract_file
from renku.ui.service.views import result_response


class UploadFilesCtrl(ServiceCtrl, RenkuOperationMixin):
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
        args = {**flask_request.args, **flask_request.form}
        self.response_builder.update(UploadFilesCtrl.REQUEST_SERIALIZER.load(args))

        super(UploadFilesCtrl, self).__init__(cache, user_data, {})

    @property
    def context(self):
        """Controller operation context."""
        return self.response_builder

    @property
    def user_cache_dir(self) -> Path:
        """The cache directory for a user."""
        directory = CACHE_UPLOADS_PATH / self.user.user_id
        directory.mkdir(exist_ok=True)

        return directory

    def process_upload(self):
        """Process an upload."""
        if self.response_builder.get("chunked_id", None) is None:
            return self.process_file()

        return self.process_chunked_upload()

    def process_chunked_upload(self):
        """Process upload done in chunks."""
        if self.response_builder["total_size"] > MAX_CONTENT_LENGTH:
            if MAX_CONTENT_LENGTH > 524288000:
                max_size = bytes_to_unit(MAX_CONTENT_LENGTH, "gb")
                max_size_str = f"{max_size} gb"
            else:
                max_size = bytes_to_unit(MAX_CONTENT_LENGTH, "mb")
                max_size_str = f"{max_size} mb"
            raise UserUploadTooLargeError(maximum_size=max_size_str, exception=None)

        chunked_id = self.response_builder["chunked_id"]

        chunks_dir: Path = self.user_cache_dir / chunked_id
        chunks_dir.mkdir(exist_ok=True, parents=True)

        current_chunk = self.response_builder["chunk_index"]
        total_chunks = self.response_builder["chunk_count"]

        file_path = chunks_dir / str(current_chunk)
        relative_path = file_path.relative_to(chunks_dir)

        self.file.save(str(file_path))

        with self.cache.model_db.lock(f"chunked_upload_{self.user.user_id}_{chunked_id}"):
            self.cache.set_file_chunk(
                self.user,
                {
                    "chunked_id": chunked_id,
                    "file_name": str(current_chunk),
                    "relative_path": str(relative_path),
                },
            )
            completed = len(list(self.cache.get_chunks(self.user, chunked_id))) == total_chunks

        if not completed:
            return {}

        target_file_path = self.user_cache_dir / self.file.filename

        if target_file_path.exists():
            if self.response_builder.get("override_existing", False):
                target_file_path.unlink()
            else:
                raise IntermittentFileExistsError(file_name=self.file.filename)

        with open(target_file_path, "wb") as target_file:
            for file_number in range(total_chunks):
                with (chunks_dir / str(file_number)).open("rb") as chunk:
                    shutil.copyfileobj(chunk, target_file)

            shutil.rmtree(chunks_dir)
            self.cache.invalidate_chunks(self.user, chunked_id)

        self.response_builder["is_archive"] = self.response_builder.get("chunked_content_type") in SUPPORTED_ARCHIVES

        return self.postprocess_file(target_file_path)

    def process_file(self):
        """Process uploaded file."""

        file_path = self.user_cache_dir / self.file.filename
        if file_path.exists():
            if self.response_builder.get("override_existing", False):
                file_path.unlink()
            else:
                raise IntermittentFileExistsError(file_name=self.file.filename)

        self.file.save(str(file_path))

        return self.postprocess_file(file_path)

    def postprocess_file(self, file_path):
        """Postprocessing of uploaded file."""
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
                relative_path = file_.relative_to(self.user_cache_dir)

                file_obj = {
                    "file_name": file_.name,
                    "file_size": os.stat(file_).st_size,
                    "relative_path": str(relative_path),
                    "is_dir": file_.is_dir(),
                }

                files.append(file_obj)

        else:
            relative_path = file_path.relative_to(self.user_cache_dir)

            file_obj = {
                "file_name": self.response_builder["file_name"],
                "file_size": os.stat(file_path).st_size,
                "relative_path": str(relative_path),
                "is_dir": file_path.is_dir(),
                "is_archive": self.response_builder["is_archive"],
            }

            files.append(file_obj)

        files = self.cache.set_files(self.user, files)
        return {"files": files}

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(UploadFilesCtrl.RESPONSE_SERIALIZER, self.process_upload())
