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
# limitations under the License..PHONY: build-docker-images push-docker-images login

DOCKER_REPOSITORY?=rengahub/
DOCKER_PREFIX:=${DOCKER_REGISTRY}$(DOCKER_REPOSITORY)

ALL_STACKS:=\
	minimal-notebook \
	r-notebook \
	scipy-notebook \
	tensorflow-notebook \
	datascience-notebook \
	pyspark-notebook \
	all-spark-notebook

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=12 --verify HEAD)

build-docker-images: $(ALL_STACKS:%=build/%)

build/%: Dockerfile.template
	cat $< | sed "s!%%NOTEBOOK_STACK%%!$(notdir $@)!g;" | docker build --rm --force-rm -t rengahub/$(notdir $@):latest -f - .

push-docker-images: $(ALL_STACKS:%=push/%)

tag/%: build/%
	docker tag $(DOCKER_PREFIX)$(notdir $@):latest $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA)

push/%: tag/%
	docker push $(DOCKER_PREFIX)$(notdir $@):latest
	docker push $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA)

login:
	@docker login -u="${DOCKER_USERNAME}" -p="${DOCKER_PASSWORD}" ${DOCKER_REGISTRY}
