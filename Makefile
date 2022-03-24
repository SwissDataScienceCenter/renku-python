# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

DOCKER_REPOSITORY?=renku/
DOCKER_PREFIX:=${DOCKER_REGISTRY}$(DOCKER_REPOSITORY)

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short --verify HEAD)

TEMPLATE_URL:=https://github.com/SwissDataScienceCenter/renku-project-template
TEMPLATE_REFERENCE:=$(shell sed -n -E 's/^__template_version__ = "([^"]+)"/\1/p' renku/version.py)
TEMPLATE_DIR:=renku/templates/

.PHONY: service cli docker-tag docker-push docker-login

docker-tag: service cli
	docker tag $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)renku-python:latest
	docker tag $(DOCKER_PREFIX)renku-core:$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)renku-core:latest

docker-push: docker-tag
	docker push $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA)
	docker push $(DOCKER_PREFIX)renku-python:latest
	docker push $(DOCKER_PREFIX)renku-core:$(GIT_MASTER_HEAD_SHA)
	docker push $(DOCKER_PREFIX)renku-core:latest

docker-login:
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin ${DOCKER_REGISTRY}

service:
	docker build -f Dockerfile.svc -t $(DOCKER_PREFIX)renku-core:`git rev-parse --short HEAD` --build-arg CLEAN_INSTALL=1 .

cli:
	docker build -f Dockerfile.cli -t $(DOCKER_PREFIX)renku-python:`git rev-parse --short HEAD` --build-arg CLEAN_INSTALL=1 .

download-templates:
	@[ "${TEMPLATE_REFERENCE}" ] || ( echo "__template_version__ is not set"; exit 1 )
	@echo "Downloading templates"
	rm -rf $(TEMPLATE_DIR)
	mkdir -p $(TEMPLATE_DIR)
	git clone $(TEMPLATE_URL) $(TEMPLATE_DIR)
	git --git-dir=$(TEMPLATE_DIR).git --work-tree=$(TEMPLATE_DIR) checkout $(TEMPLATE_REFERENCE)
	rm -rf $(TEMPLATE_DIR).git

build-package: download-templates
	@echo "Building package"
	poetry build

publish-package: build-package
	@echo "Publishing package"
	poetry config pypi-token.pypi ${PYPI_TOKEN}
	poetry publish

build-lock-package: download-templates
	@echo "Building package"
	poetry-dynamic-versioning
	poetry run poetry-lock-package --build
	sed -i --regexp-extended 's/^version\s=\s"[^"]+"/version = "0.0.0"/gm' pyproject.toml
	sed -i --regexp-extended 's/^__version__\s=\s"[^"]+"/__version__ = "0.0.0"/gm' renku/version.py

publish-lock-package: build-lock-package
	@echo "Publishing package"
	twine upload -u __token__ -p ${PYPI_TOKEN} dist/renku_lock*
