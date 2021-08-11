#!/bin/bash
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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

set -e

COLOR_RED="\033[0;31m"
COLOR_RESET="\033[0m"

CURRENT_CONTEXT=$(kubectl config current-context)
if [[ ! $CURRENT_CONTEXT ]]
then
    read -p "No default kubernetes context. Please specify one: " -r
    CURRENT_CONTEXT=$REPLY
else
    echo -e "Your current kubernetes context is: ${COLOR_RED}${CURRENT_CONTEXT}${COLOR_RESET}"
    read -p "Press enter to use it, or type a different one [skip]: " -r
    if [[ $REPLY ]]
    then
        CURRENT_CONTEXT=$REPLY
    fi
fi

if [[ ! $DEV_NAMESPACE ]]
then
    read -p "No dev namespace found. Please specify one: " -r
    DEV_NAMESPACE=$REPLY
else
    echo -e "Your current dev namespace is: ${COLOR_RED}${DEV_NAMESPACE}${COLOR_RESET}"
    read -p "Press enter to use it, or type a different one [skip]: " -r
    if [[ $REPLY ]]
    then
        DEV_NAMESPACE=$REPLY
    fi
fi

if [[ ! $CURRENT_CONTEXT ]] || [[ ! $DEV_NAMESPACE ]]
then
    echo "ERROR: you need to provide a context and a namespace"
    exit 1
fi

# Create local directory for service cache
if [[ ! -d "temp" ]]
then
    mkdir temp 
fi
if [[ ! -d "temp/service_cache" ]]
then
    mkdir temp/service_cache 
fi

POD_NAME="${DEV_NAMESPACE}-renku-core"
echo -e ""
echo -e "Context: ${COLOR_RED}${CURRENT_CONTEXT}${COLOR_RESET}, target: ${COLOR_RED}${POD_NAME}${COLOR_RESET}"
echo "Starting telepresence..."
echo -e ""
echo "********** INSTRUCTIONS **********"
echo -e ""
echo -e "\U0001F511 Please enter the password when required."
echo -e ""
echo -e "\U0001F5A5  When the command line is ready, manually execute the following command."
echo "Be sure to be in the local python context where you develop renku-python."
echo -e ""
echo ">>> COMMAND BEGIN <<<"
echo "CACHE_DIR=temp/service_cache \
DEBUG_MODE=true DEBUG=1 FLASK_DEBUG=1 \
FLASK_ENV=development FLASK_APP=renku.service.entrypoint \
flask run --no-reload"
echo ">>> COMMAND END <<<"
echo -e ""
echo -e "\U0001F50D You can tests if the service is running properly from your browser:"
echo "https://${DEV_NAMESPACE}.dev.renku.ch/api/renku/version."
echo -e ""
echo -e "\U0001F40D You should be able to attach a remote python debugger."
echo "If you use VScode, be sure to have the following settings:"
echo '"type": "python", "request": "attach", "port": 5678, "host": "localhost"'
echo -e ""
echo -e "\U0001F438 Enjoy Renku!"
echo -e ""

telepresence \
    --swap-deployment "${POD_NAME}" \
    --namespace "${DEV_NAMESPACE}" \
    --expose 5000:8080 \
    --run-shell
