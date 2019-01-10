import yaml
import logging
from lando.exceptions import get_or_raise_config_exception, InvalidConfigException
from lando.server.config import WorkQueue, CloudSettings, BespinApiSettings


class ServerConfig(object):
    """
    Configuration for either Server or Client.
    For worker, cloud_settings will return None.
    """
    def __init__(self, filename):
        """
        Parse yaml file and store internal data .
        :param filename: str: path to a yaml config file
        """
        with open(filename, 'r') as infile:
            data = yaml.load(infile)
            if not data:
                raise InvalidConfigException("Empty config file {}.".format(filename))
            self.work_queue_config = WorkQueue(get_or_raise_config_exception(data, 'work_queue'))
            self.cluster_api = self._optional_get(data, 'cluster_api', ClusterApiSettings)
            self.bespin_api_settings = self._optional_get(data, 'bespin_api', BespinApiSettings)
            self.log_level = data.get('log_level', logging.WARNING)

    @staticmethod
    def _optional_get(data, name, constructor):
        value = data.get(name, None)
        if value:
            return constructor(value)
        else:
            return None


class ClusterApiSettings(object):
    """
    Settings used to talk to be Bespin job api.
    """
    def __init__(self, data):
        self.host = get_or_raise_config_exception(data, 'host')
        self.token = get_or_raise_config_exception(data, 'token')
        self.namespace = get_or_raise_config_exception(data, 'namespace')
