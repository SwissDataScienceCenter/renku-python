# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Template tests."""

import textwrap

import pytest

from renku.core import errors
from renku.core.util.metadata import read_renku_version_from_dockerfile
from renku.domain_model.template import TemplateMetadata, TemplateParameter, TemplatesManifest

TEMPLATE_METADATA = {"__name__": "my-project", "__project_description__": "My Project", "__renku_version__": "42.0.0"}


def test_template_get_files(source_template):
    """Test get files of a template."""
    files = set(source_template.get_files())

    assert {
        ".gitignore",
        ".renku/renku.ini",
        "Dockerfile",
        "README.md",
        "{{ __name__ }}.dummy",
        "requirements.txt",
        "immutable.file",
    } == files


def test_template_render(source_template):
    """Test rendering a template."""
    rendered_template = source_template.render(metadata=TemplateMetadata.from_dict(TEMPLATE_METADATA))

    assert "A Renku project: My Project\n" == (rendered_template.path / "README.md").read_text()
    assert "42.0.0" == str(read_renku_version_from_dockerfile(rendered_template.path / "Dockerfile"))


@pytest.mark.parametrize("name", ["", "a-renku-project"])
def test_template_render_with_templated_filename(source_template, name):
    """Test rendering a template with templated filenames."""
    rendered_template = source_template.render(metadata=TemplateMetadata.from_dict({"__name__": name}))

    assert (rendered_template.path / f"{name}.dummy").exists()


def test_template_get_rendered_files(source_template):
    """Test get files of a rendered template."""
    rendered_template = source_template.render(metadata=TemplateMetadata.from_dict(TEMPLATE_METADATA))

    assert {
        ".gitignore",
        ".renku/renku.ini",
        "Dockerfile",
        "README.md",
        "my-project.dummy",
        "requirements.txt",
        "immutable.file",
    } == set(rendered_template.get_files())


def test_templates_manifest():
    """Test creating a template manifest."""
    manifest = TemplatesManifest.from_string(
        textwrap.dedent(
            """
            - folder: python
              name: Python Project
              description: A Python-based Renku project
              variables: {}
              icon: python.png
            - id: R
              name: R Project
              description: An R-based Renku project
              variables:
                rate:
                  type: number
                  description: sample rate
              icon: R.png
            """
        )
    )

    assert 2 == len(manifest.templates)

    template = next(t for t in manifest.templates if t.id == "python")
    assert "Python Project" == template.name
    assert "A Python-based Renku project" == template.description
    assert "python.png" == template.icon
    assert [] == template.parameters

    template = next(t for t in manifest.templates if t.id == "R")
    assert "R Project" == template.name
    assert "An R-based Renku project" == template.description
    assert "R.png" == template.icon
    assert 1 == len(template.parameters)
    assert "rate" == template.parameters[0].name
    assert "number" == template.parameters[0].type
    assert "sample rate" == template.parameters[0].description


def test_templates_manifest_non_existing_file():
    """Test creating a template manifest form non-existing file."""
    with pytest.raises(errors.InvalidTemplateError, match="There is no manifest file 'non-existing-path'"):
        TemplatesManifest.from_path("non-existing-path")


def test_templates_manifest_binary_content(tmp_path):
    """Test creating a template manifest form non-text file."""
    path = tmp_path / "manifest.yaml"
    path.write_bytes(b"\x80")  # NOTE: Write an invalid unicode sequence
    with pytest.raises(errors.InvalidTemplateError, match="Cannot read manifest file.*manifest.yaml"):
        TemplatesManifest.from_path(path)


def test_templates_manifest_invalid_yaml(tmp_path):
    """Test creating a template manifest form invalid YAML content."""
    with pytest.raises(errors.InvalidTemplateError, match="Cannot parse manifest file"):
        TemplatesManifest.from_string("- id: python\nid")


@pytest.mark.parametrize(
    "content, message",
    [
        ("", "Cannot find any valid template in manifest file"),
        ("id: python", "Invalid manifest content type: 'dict'"),
        ("-\n  - id: python", "Invalid template type: 'list'"),
        ("- no-id: python", "Template doesn't have an id:"),
        ("- id: python\n  variables: p1", "Invalid template variable type on template 'python': 'str'"),
        ("- id: python\n  variables:\n    p1: 42", "Invalid parameter type 'int' for 'p1'"),
    ],
)
def test_templates_manifest_invalid_content(tmp_path, content, message):
    """Test creating a template manifest form invalid content."""
    with pytest.raises(errors.InvalidTemplateError, match=message):
        TemplatesManifest.from_string(content)


def test_templates_manifest_warnings(tmp_path):
    """Test creating a template manifest form invalid content."""
    content = "- folder: python\n  name: python\n  variables:\n    p1: My parameter"
    manifest = TemplatesManifest.from_string(content, skip_validation=True)
    warnings = manifest.validate()

    assert "Template 'python' should use 'id' attribute instead of 'folder'." in warnings
    assert (
        "Template 'python' variable 'p1' uses old string format in manifest and should be replaced"
        " with the nested dictionary format."
    ) in warnings


@pytest.mark.parametrize("default, has_default", [(None, False), (42, True), ("", True), (False, True)])
def test_template_parameter_default_value(default, has_default):
    """Test parameter has not default only if default is None."""
    parameter = TemplateParameter(name="parameter", description="", type="", default=default, possible_values=None)
    parameter.validate()

    assert default == parameter.default
    assert parameter.has_default is has_default


@pytest.mark.parametrize(
    "value, message",
    [
        ({"type": "int"}, "Template contains variable .* of type 'int' which is not supported"),
        ({"possible_values": "42"}, "Invalid type for possible values of template variable"),
        ({"type": "enum"}, "Template variable 'parameter' of type enum does not provide a corresponding enum list"),
        ({"type": "number", "default_value": "true"}, "Invalid default value for 'parameter':"),
    ],
)
def test_template_parameter_validation(value, message):
    """Test TemplateVariable validations."""
    with pytest.raises(errors.InvalidTemplateError, match=message):
        parameter = TemplateParameter.from_dict(name="parameter", value=value)
        parameter.validate()


@pytest.mark.parametrize(
    "type, possible_values, value, expected_value, expected_type",
    [
        (None, None, "truE", "truE", str),
        (None, None, True, True, bool),
        (None, None, 42, 42, int),
        (None, None, None, None, type(None)),
        ("boolean", None, "true", True, bool),
        ("boolean", None, True, True, bool),
        ("boolean", None, "False", False, bool),
        ("number", None, 42, 42, int),
        ("number", None, "42", 42, int),
        ("number", None, "42.0", 42, float),
        ("string", None, "", "", str),
        ("string", None, "some value", "some value", str),
        ("enum", ["1", "2", "3"], "2", "2", str),
    ],
)
def test_template_parameter_value_conversion(type, possible_values, value, expected_value, expected_type):
    """Test TemplateVariable conversion."""
    parameter = TemplateParameter.from_dict(name="parameter", value={"type": type, "possible_values": possible_values})

    converted_value = parameter.convert(value)

    assert expected_value == converted_value
    assert expected_type == converted_value.__class__


@pytest.mark.parametrize(
    "type, possible_values, value",
    [
        ("boolean", None, "TRUE"),
        ("boolean", None, 42),
        ("boolean", None, ""),
        ("number", None, "42.0f"),
        ("string", None, 42),
        ("enum", ["1", "2", "3"], "42"),
    ],
)
def test_template_parameter_value_conversion_error(type, possible_values, value):
    """Test TemplateVariable conversion with invalid values."""
    parameter = TemplateParameter.from_dict(name="parameter", value={"type": type, "possible_values": possible_values})

    with pytest.raises(ValueError, match=f"Invalid value '{value}.*' for template variable 'parameter.*'"):
        parameter.convert(value)
