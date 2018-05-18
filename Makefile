# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
DOCKER_LABEL?=$(or ${TRAVIS_BRANCH},${TRAVIS_BRANCH},$(shell git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/^* //'))

ifeq ($(DOCKER_LABEL), master)
	DOCKER_LABEL=latest
endif

ALL_STACKS:=\
	minimal-notebook \
	r-notebook \
	scipy-notebook \
	tensorflow-notebook \
	datascience-notebook \
	pyspark-notebook \
	all-spark-notebook

IMAGES=$(ALL_STACKS) gitlab-runner renku-python

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=12 --verify HEAD)

tag-docker-images: $(IMAGES:%=tag/%)

build/%-notebook: Dockerfile.notebook.template
	cat $< | sed "s!%%NOTEBOOK_STACK%%!$(notdir $@)!g;" | docker build --rm --force-rm -t renku/$(notdir $@):$(GIT_MASTER_HEAD_SHA) -f - .

build/%: Dockerfile.%
	docker build --rm --force-rm -t renku/$(notdir $@):$(GIT_MASTER_HEAD_SHA) -f $< .

push-docker-images: $(IMAGES:%=push/%)

tag/%: build/%
	docker tag $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)$(notdir $@):$(DOCKER_LABEL)

push/%: tag/%
	docker push $(DOCKER_PREFIX)$(notdir $@):$(DOCKER_LABEL)
	docker push $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA)

login:
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin ${DOCKER_REGISTRY}
