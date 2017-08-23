#!/bin/sh

# quit on errors:
set -o errexit

# quit on unbound symbols:
set -o nounset

DIR=`dirname "$0"`

cd $DIR
export FLASK_APP=app.py

# Teardown app
[ -e "$DIR/instance" ] && rm -Rf $DIR/instance
