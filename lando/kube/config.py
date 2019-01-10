import yaml
import logging
from lando.exceptions import get_or_raise_config_exception, InvalidConfigException
import os


class ServerConfig(object):
    """
    Configuration for either Server or Client.
    For worker, cloud_settings will return None.
    """
    def __init__(self):
        """
        Parse yaml file and store internal data .
        :param filename: str: path to a yaml config file
        """
        self.work_queue_config = WorkQueue()
        self.cluster_api = ClusterApiSettings()
        self.bespin_api_settings = BespinApiSettings()
        self.log_level = os.environ.get('LOG_LEVEL', logging.WARNING)

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
    def __init__(self):
        self.host = os.environ.get('BESPIN_CLUSTER_HOST')
        self.token = os.environ.get('BESPIN_CLUSTER_TOKEN')
        self.namespace = os.environ.get('BESPIN_CLUSTER_NAMESPACE')
        self.incluster_config = os.environ.get('BESPIN_INCLUSTER_CONFIG')


class WorkQueue(object):
    """
    Settings for the AMQP used to control lando_worker processes.
    """
    def __init__(self):
        self.host = os.environ['BESPIN_RABBIT_HOST']
        self.username = os.environ.get('BESPIN_RABBIT_USERNAME')
        self.password = os.environ.get('BESPIN_RABBIT_PASSWORD')
        self.listen_queue = os.environ['BESPIN_RABBIT_QUEUE']


class BespinApiSettings(object):
    """
    Settings used to talk to be Bespin job api.
    """
    def __init__(self):
        self.url = os.environ['BESPIN_API_URL']
        self.token = os.environ['BESPIN_API_TOKEN']
