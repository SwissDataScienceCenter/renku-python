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

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=12 --verify HEAD)

.PHONY: docker-build docker-tag docker-push docker-login

docker-build: Dockerfile
	docker build --rm --force-rm -t $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA) -f $< .

docker-tag: docker-build
	docker tag $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)renku-python:$(DOCKER_LABEL)

docker-push: docker-tag
	docker push $(DOCKER_PREFIX)renku-python:$(DOCKER_LABEL)
	docker push $(DOCKER_PREFIX)renku-python:$(GIT_MASTER_HEAD_SHA)

docker-login:
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin ${DOCKER_REGISTRY}

renku.rb: Pipfile.lock brew.py
	@python brew.py renku > $@

.PHONY: brew-commit-formula
brew-commit-formula: renku.rb
	brew tap swissdatasciencecenter/renku
	brew formula renku
	cp $< /usr/local/Homebrew/Library/Taps/swissdatasciencecenter/homebrew-renku/renku.rb
	cd $(shell brew --repo swissdatasciencecenter/renku) && git commit -a -m "renku: release $(shell brew info --json=v1 renku | jq -r '.[0].versions.stable')"

.PHONY: brew-build-bottle
brew-build-bottle:
	@brew uninstall renku || echo OK
	@brew tap swissdatasciencecenter/renku
	@brew install --build-bottle renku

%.bottle.json:
	@brew bottle --json --root-url=https://github.com/SwissDataScienceCenter/renku-python/releases/download/v$(shell brew info --json=v1 renku | jq -r '.[0].versions.stable') renku || echo 'OK'
	@echo "Renaming:"
	@cat $@ | jq -r '.[].bottle.tags[] | select(.local_filename != .filename) | "- \"\(.local_filename)\" to \"\(.filename)\""'
	@cat $@ | jq -r '.[].bottle.tags[] | select(.local_filename != .filename) | "mv \"\(.local_filename)\" \"\(.filename)\""' | sh || echo "Move was not successfull."

.PHONY: brew-commit-bottle
brew-commit-bottle: *.bottle.json
	brew bottle --merge --keep-old --write $< || brew bottle --merge --write $<

brew-release:
	open "https://github.com/SwissDataScienceCenter/renku-python/releases/new?tag=v$(shell brew info --json=v1 renku | jq -r '.[0].versions.stable')"
