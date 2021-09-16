# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""test KG against SHACL shape."""

import pyld
import pytest

from renku.cli import cli
from renku.core.compat import Path
from renku.core.utils.shacl import validate_graph
from tests.utils import load_dataset


@pytest.mark.skip(reason="FIXME correct this when implementing renku graph export")
def test_dataset_shacl(tmpdir, runner, project, client):
    """Test dataset metadata structure."""
    force_dataset_path = Path(__file__).parent.parent.parent / "data" / "force_dataset_shacl.json"

    force_datasetfile_path = Path(__file__).parent.parent.parent / "data" / "force_datasetfile_shacl.json"

    force_datasettag_path = Path(__file__).parent.parent.parent / "data" / "force_datasettag_shacl.json"

    runner.invoke(cli, ["dataset", "create", "dataset"])

    paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    runner.invoke(cli, ["dataset", "add", "dataset"] + paths, catch_exceptions=False)

    runner.invoke(cli, ["dataset", "tag", "dataset", "1.0"], catch_exceptions=False)

    dataset = load_dataset(client, "dataset")
    g = dataset.to_jsonld()
    rdf = pyld.jsonld.to_rdf(g, options={"format": "application/n-quads", "produceGeneralizedRdf": True})

    r, _, t = validate_graph(rdf, shacl_path=str(force_dataset_path))
    assert r is True, t

    r, _, t = validate_graph(rdf, shacl_path=str(force_datasetfile_path))
    assert r is True, t

    r, _, t = validate_graph(rdf, shacl_path=str(force_datasettag_path))
    assert r is True, t

    r, _, t = validate_graph(rdf)
    assert r is True, t


def test_project_shacl(project, client, client_database_injection_manager):
    """Test project metadata structure."""
    from renku.core.models.project import ProjectSchema
    from renku.core.models.provenance.agent import Person

    path = Path(__file__).parent.parent.parent / "data" / "force_project_shacl.json"

    with client_database_injection_manager(client):
        project = client.project
        project.creator = Person(email="johndoe@example.com", name="Johnny Doe")

        g = ProjectSchema().dump(project)

    g["@id"] = "https://localhost/" + g["@id"]
    rdf = pyld.jsonld.to_rdf(g, options={"format": "application/n-quads", "produceGeneralizedRdf": False})
    r, _, t = validate_graph(rdf, shacl_path=str(path))
    assert r is True, t

    r, _, t = validate_graph(rdf)
    assert r is True, t
