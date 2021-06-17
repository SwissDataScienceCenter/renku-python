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
