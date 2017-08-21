#!/usr/bin/env sh
# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#

pydocstyle renga tests docs && \
isort -rc -c -df && \
check-manifest --ignore ".travis-*" && \
sphinx-build -qnNW docs docs/_build/html && \
python setup.py test
