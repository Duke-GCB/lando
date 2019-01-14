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
        self.worker_image_name = os.environ['LANDO_WORKER_IMAGE_NAME']
        self.log_level = os.environ.get('LOG_LEVEL', logging.WARNING)


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
        self.username = os.environ.get('BESPIN_QUEUE_LANDO_USERNAME')
        self.password = os.environ.get('BESPIN_QUEUE_LANDO_PASSWORD')
        self.worker_username = os.environ.get('BESPIN_QUEUE_WORKER_USERNAME')
        self.worker_password = os.environ.get('BESPIN_QUEUE_WORKER_PASSWORD')
        self.listen_queue = os.environ['BESPIN_RABBIT_QUEUE']


class BespinApiSettings(object):
    """
    Settings used to talk to be Bespin job api.
    """
    def __init__(self):
        self.url = os.environ['BESPIN_API_URL']
        self.token = os.environ['BESPIN_API_TOKEN']
