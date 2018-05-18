#!/bin/bash -e
renku notebooks configure
tini -- $@
