#!/bin/sh

# if running in debug mode, install editable
if [ "${DEBUG_MODE}" = "true" ]; then
    pip3 install --user --no-use-pep517 -e ".[service]"
fi

renku service "$@"
