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
"""Dataset domain model tests."""

import pytest

from renku.core import errors
from renku.domain_model.dataset import Dataset, is_dataset_slug_valid


@pytest.mark.parametrize(
    "slug", ["lowercase", "UPPERCASE", "PascalCase", "12345", "with-dashes", "with_underlines", "with.dots"]
)
def test_valid_dataset_slug(slug):
    """Test several valid dataset slug."""
    assert is_dataset_slug_valid(slug)


@pytest.mark.parametrize(
    "invalid_slug",
    [
        None,
        "",  # Empty string
        "nön-ascii",
        "with spaces",
        "invalid:characters",
        "_starts-with-underline",
        "ends-with-underline_",
        "-starts-with-dash",
        "ends-with-dash-",
        ".starts-with-dot",
        "ends-with-dot.",
        "double__underlines",
        "double--dashes",
        "double..dots",
        "back-to-back-_non-alphanumeric",
        "ends-with.lock",
    ],
)
def test_invalid_dataset_slug(invalid_slug):
    """Test several invalid dataset slug."""
    assert not is_dataset_slug_valid(invalid_slug)


def test_load_existing_metadata_in_new_model():
    """
    Test that existing projects metadata can be correctly loaded in the new dataset model that replaces name/title with
    slug/name.
    """
    dataset = Dataset.__new__(Dataset)

    dataset.__setstate__({"name": "machine-friendly-slug", "title": "Human Readable Name", "dataset_files": []})

    assert "machine-friendly-slug" == dataset.slug
    assert "Human Readable Name" == dataset.name
    assert dataset.title is None


@pytest.mark.parametrize("name, title", [("", "Human Readable Name"), ("Human Readable Name", "")])
def test_create_dataset_from_existing_metadata(name, title):
    """Test that datasets can be correctly created using existing projects metadata where slug has no value."""
    dataset = Dataset(name=name, title=title)

    assert "human_readable_name" == dataset.slug
    assert "Human Readable Name" == dataset.name
    assert dataset.title is None


@pytest.mark.parametrize("slug", ["", None])
def test_slug_is_created_from_name_when_missing(slug):
    """Test that when no slug is passed to the constructor, one is automatically created from the name."""
    dataset = Dataset(slug=slug, name="Human Readable Name")

    assert "human_readable_name" == dataset.slug
    assert "Human Readable Name" == dataset.name
    assert dataset.title is None


@pytest.mark.parametrize("name", ["", None])
def test_slug_is_not_copied_to_name_when_name_is_missing(name):
    """Test that dataset doesn't use slug as name when name isn't passed."""
    dataset = Dataset(slug="machine-friendly-slug", name=name)

    assert "machine-friendly-slug" == dataset.slug
    assert name == dataset.name
    assert dataset.title is None


@pytest.mark.parametrize("name", ["", None, "Human Readable Name"])
def test_mixed_metadata_adjusts_name_and_title(name):
    """Test that a mixed metadata where slug and title are both present, uses title as name."""
    dataset = Dataset(slug="machine-friendly-slug", name=name, title="Dataset Title")

    assert "machine-friendly-slug" == dataset.slug
    assert "Dataset Title" == dataset.name
    assert dataset.title is None


def test_creation_fails_when_slug_and_name_and_title_are_missing():
    """At least one of the slug, name, or title should have a value."""
    with pytest.raises(errors.ParameterError):
        Dataset(slug=None, name=None, title=None)
