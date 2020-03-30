# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
# limitations under the License..PHONY: build-docker-images push-docker-images login

DOCKER_REPOSITORY?=renku/
DOCKER_PREFIX:=${DOCKER_REGISTRY}$(DOCKER_REPOSITORY)

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=12 --verify HEAD)

.PHONY: docker-build docker-tag docker-push docker-login

docker-build: Dockerfile.cli
	docker build --rm --force-rm -t $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA) -f $< .

docker-tag: docker-build
	docker tag $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)renku-python:latest

docker-push: docker-tag
	docker push $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA)
	docker push $(DOCKER_PREFIX)renku-python:latest

docker-login:
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin ${DOCKER_REGISTRY}

service-container:
	docker build -f Dockerfile.svc -t renku-core:`git rev-parse --short HEAD` .
