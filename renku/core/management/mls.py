import uuid
from contextlib import contextmanager
from pathlib import Path

from renku.core.models.mls import Run
from renku.core.models.locals import with_reference
from renku.core.models.refs import LinkReference


class MLSApiMixin(object):
    """Client for handling ML Schema."""

    MLS = 'ml'
    METADATA_FILE = 'metadata.jsonld'
    """Directory for storing MLS metadata in Renku."""

    @property
    def renku_mls_path(self):
        """Return a ``Path`` instance of Renku MLS metadata folder."""
        return Path(self.renku_home).joinpath(self.MLS)

    def get_model(self, path):
        """Return an MLS model from a given path."""
        if not path.is_absolute():
            path = self.path / path
        return Run.from_jsonld(path, client=self)

    def model_path(self, name):
        """Get MLS path from name."""
        path = self.renku_mls_path / name / self.METADATA_FILE
        if not path.exists():
            path = LinkReference(
                client=self, name='ml/' + name
            ).reference

        return path

    def load_model(self, name=None):
        """Load MLS reference file."""
        path = None
        model = None

        if name:
            path = self.model_path(name)
            if path.exists():
                model = self.get_model(path)

        return model

    @contextmanager
    def with_model(self, name=None):
        """Yield an editable metadata object for a model."""

        model = self.load_model(name=name)

        if model is None:
            identifier = str(uuid.uuid4())
            path = (self.renku_mls_path / identifier / self.METADATA_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)

            with with_reference(path):
                model = Run(
                    identifier=identifier, name=name, client=self
                )

            if name:
                LinkReference.create(client=self, name='ml/' +
                                     name).set_reference(path)

        model_path = self.path / self.datadir / model.name
        model_path.mkdir(parents=True, exist_ok=True)

        yield model
