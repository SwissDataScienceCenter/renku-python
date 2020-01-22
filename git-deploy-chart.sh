#!/usr/bin/env bash
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

set -ex

# get ssh key to use for docker hub login
chmod 600 deploy_rsa
eval "$(ssh-agent -s)"
ssh-add deploy_rsa

make docker-login

# build charts/images and push
helm repo update
helm dependency update helm-chart/renku-core
chartpress --push --publish-chart
git diff

# push also images tagged with "latest"
chartpress --tag latest --push

# if it's a tag, push the tagged chart
if [[ -n $TAG ]]; then
    git clean -dff
    chartpress --tag "$TAG" --push --publish-chart
fi
