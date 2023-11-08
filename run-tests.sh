#!/usr/bin/env sh
# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

# quit on errors:
set -o errexit

# quit on unbound symbols:
set -o nounset

USAGE=$(cat <<-END
	Usage:
	    run-tests.sh [-s | --styles] [-d | --docs] [-t | --tests]
	    run-tests.sh -h | --help
	Options:
	    -h, --help		Show this screen.
	    -s, --styles	check styles
	    -d, --docs		build and test docs
	    -t, --tests		run unit tests
END
)

check_styles(){
    pydocstyle renku tests conftest.py docs
    black --check --diff renku tests conftest.py
    isort -c --df .
    flake8 renku/ tests/ conftest.py
    find . -path ./.eggs -prune -o -iname \*.sh -print0 | xargs -0 shellcheck
    poetry lock --no-update && (git diff --exit-code -- poetry.lock > /dev/null || (echo "Poetry lock file out of date! Run 'poetry lock'"))
}

build_docs(){
    sphinx-build -qnNW docs docs/_build/html
    sphinx-build -nNW -b spelling -d docs/_build/doctrees docs docs/_build/spelling
    pytest --black --flake8 -v -m "not integration and not publish" -o testpaths="docs conftest.py" --ignore=docs/conf.py
}

run_tests(){
    pytest -v -m "not integration and not publish and not redis" -o testpaths="tests renku conftest.py" --ignore=renku/version.py
}

usage(){
    echo "$USAGE"
}

all=1
tests=
docs=
styles=

while [ "${1-}" != "" ]; do
    case $1 in
        -t | --tests )
            tests=1
            all=0
            ;;
        -d | --docs )
            docs=1
            all=0
            ;;
        -s | --styles )
            styles=1
            all=0
            ;;
        -h | --help )
            usage
            exit
            ;;
    esac
    shift
done

docs=$((docs||all))
tests=$((tests||all))
styles=$((styles||all))

if [ "$docs" = "1" ]; then
    build_docs
fi

if [ "$styles" = "1" ]; then
    check_styles
fi

if [ "$tests" = "1" ]; then
    run_tests
fi
