import configparser
import os
import attr

from renku._compat import Path


@attr.s
class ConfigApiMixin:
    """Client for handling global configuration."""

    user_dir = attr.ib(default=os.path.expanduser('~'), converter=str)
    config_dir = attr.ib(default='.renku', converter=str)
    config_name = attr.ib(default='renku.ini', converter=str)

    @property
    def renku_config_path(self):
        config = Path(self.user_dir) / Path(self.config_dir)
        if config.exists() is False:
            config.mkdir()

        return config / Path(self.config_name)

    def load_config(self):
        """Loads global configuration object."""
        config = configparser.ConfigParser()
        config.read(self.renku_config_path)
        return config

    def store_config(self, config):
        """Persists global configuration object."""
        with open(self.renku_config_path, 'w') as file:
            config.write(file)

    def load_secret(self, provider):
        """Load a secret for a external provider."""
        try:
            config = self.load_config()
            return config[provider]['secret']
        except KeyError:
            return None

    def store_secret(self, provider, secret):
        """Store secret for a provider."""
        config = self.load_config()
        if provider in config:
            config[provider]['secret'] = secret
        else:
            config[provider] = {'secret': secret}
        self.store_config(config)

