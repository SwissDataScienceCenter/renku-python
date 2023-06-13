#!/bin/bash

set -e

core_version=$1

mapfile -t pod_ips < <(kubectl -n renku get pods --selector="app.kubernetes.io/name=core" --selector="app.kubernetes.io/deploymentVersion=$core_version" -o=jsonpath="{.items[*].status.podIP}" )

success=true

for pod_ip in "${pod_ips[@]}"
do
    if curl "http://$pod_ip/renku/cache.cleanup" ; then
        :
    else
        echo "Cleanup failed for pod $pod_ip with status $?">&2
        success=false
    fi
done

if ! $success; then
    exit 1;
fi
