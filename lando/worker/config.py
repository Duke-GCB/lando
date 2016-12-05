"""
Configuration for for use with lando_worker.
"""
import yaml
from lando.exceptions import InvalidConfigException, get_or_raise_config_exception


class WorkerConfig(object):
    """
    Contains settings for allowing lando_worker to receive messages from a queue.
    """
    def __init__(self, filename):
        """
        Parse filename setting member values.
        Raises InvalidConfigException when configuration is incorrect.
        :param filename: str: path to a yaml config file (see sample_files/workerconfig.yml)
        """
        self.filename = filename
        with open(self.filename, 'r') as infile:
            data = yaml.load(infile)
            if not data:
                raise InvalidConfigException("Empty config file {}.".format(self.filename))
            self.host = get_or_raise_config_exception(data, 'host')
            self.username = get_or_raise_config_exception(data, 'username')
            self.password = get_or_raise_config_exception(data, 'password')
            self.queue_name = get_or_raise_config_exception(data, 'queue_name')
            self.cwl_base_command = data.get('cwl_base_command', None)

    def work_queue_config(self):
        return self