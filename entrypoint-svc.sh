#!/bin/sh

# if running in debug mode, install editable
if [ "${DEBUG_MODE}" = "true" ]; then
    echo "DEBUG MODE: installing renku as editable"
    pip3 install --user --no-use-pep517 -e ".[service]"
fi

renku service "$@"
