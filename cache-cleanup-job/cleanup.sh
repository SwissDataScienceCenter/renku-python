#!/bin/bash

set -e

core_version=$1
namespace=$2

mapfile -t -d " " pod_ips < <(kubectl -n "$namespace" get pods --selector="app.kubernetes.io/name=core" --selector="app.kubernetes.io/deploymentVersion=$core_version" -o=jsonpath="{.items[*].status.podIP}" )

success=true

for pod_ip in "${pod_ips[@]}"
do
    echo "Calling http://$pod_ip:8080/renku/cache.cleanup"
    if curl "http://$pod_ip:8080/renku/cache.cleanup" ; then
        :
    else
        echo "Cleanup failed for pod $pod_ip with status $?">&2
        success=false
    fi
done

if ! $success; then
    exit 1;
fi
