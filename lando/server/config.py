"""
Reads configuration settings from a YAML file for use with lando
"""
from __future__ import absolute_import
import yaml
from lando.exceptions import get_or_raise_config_exception, InvalidConfigException


class ServerConfig(object):
    """
    Configuration for either Server or Client.
    For worker vm_settings and cloud_settings will return None.
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
            self.fake_cloud_service = data.get('fake_cloud_service', False)
            self.work_queue_config = WorkQueue(get_or_raise_config_exception(data, 'work_queue'))
            self.vm_settings = self._optional_get(data, 'vm_settings', VMSettings)
            self.cloud_settings = self._optional_get(data, 'cloud_settings', CloudSettings)
            self.bespin_api_settings = self._optional_get(data, 'bespin_api', BespinApiSettings)

    @staticmethod
    def _optional_get(data, name, constructor):
        value = data.get(name, None)
        if value:
            return constructor(value)
        else:
            return None

    def make_worker_config_yml(self, queue_name):
        """
        Create a worker config file that can be sent to a worker VM so they can respond to messages on queue_name.
        :param queue_name: str: name of the queue the worker will listen on.
        :return: str: worker config file data
        """
        work_queue = self.work_queue_config
        data = {
            'host': work_queue.host,
            'username': work_queue.worker_username,
            'password': work_queue.worker_password,
            'queue_name': queue_name
        }
        if not self.fake_cloud_service:
            data['cwl_base_command'] = self.vm_settings.cwl_base_command
        return yaml.safe_dump(data, default_flow_style=False)


class WorkQueue(object):
    """
    Settings for the AMQP used to control lando_worker processes.
    """
    def __init__(self, data):
        self.host = get_or_raise_config_exception(data, 'host')
        self.username = get_or_raise_config_exception(data, 'username')
        self.password = get_or_raise_config_exception(data, 'password')
        self.worker_username = get_or_raise_config_exception(data, 'worker_username')
        self.worker_password = get_or_raise_config_exception(data, 'worker_password')
        self.listen_queue = get_or_raise_config_exception(data, 'listen_queue')


class VMSettings(object):
    """
    Settings used to create a VM for running a job on.
    """
    def __init__(self, data):
        self.worker_image_name = get_or_raise_config_exception(data, 'worker_image_name')
        self.ssh_key_name = get_or_raise_config_exception(data, 'ssh_key_name')
        self.network_name = get_or_raise_config_exception(data, 'network_name')
        self.allocate_floating_ips = data.get('allocate_floating_ips', False)
        self.floating_ip_pool_name = get_or_raise_config_exception(data, 'floating_ip_pool_name')
        self.default_favor_name = get_or_raise_config_exception(data, 'default_favor_name')
        self.cwl_base_command = data.get("cwl_base_command")
        self.volume_mounts = data.get("volume_mounts", {})


class CloudSettings(object):
    """
    Settings used to connect to the VM provider.
    """
    def __init__(self, data):
        self.auth_url = get_or_raise_config_exception(data, 'auth_url')
        self.username = get_or_raise_config_exception(data, 'username')
        self.user_domain_name = get_or_raise_config_exception(data, 'user_domain_name')
        self.project_domain_name = get_or_raise_config_exception(data, 'project_domain_name')
        self.password = get_or_raise_config_exception(data, 'password')

    def credentials(self, project_name):
        """
        Make credentials for connecting to the cloud.
        :param project_name: str: name of the project(tenant) which will contain our VMs
        :return: dict: cloud credentials read from config file
        """
        return {
          'auth_url': self.auth_url,
          'username': self.username,
          'user_domain_name': self.user_domain_name,
          'project_name' : project_name,
          'project_domain_name': self.project_domain_name,
          'password': self.password,
        }


class BespinApiSettings(object):
    """
    Settings used to talk to be Bespin job api.
    """
    def __init__(self, data):
        self.url = get_or_raise_config_exception(data, 'url')
        self.token = get_or_raise_config_exception(data, 'token')


