"""
Reads configuration settings from a YAML file for use with lando.py and lando_client.py.
"""
import yaml


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
            self.fake_cloud_service = data.get('fake_cloud_service', False)
            self.work_queue_config = WorkQueue(data['work_queue'])
            self.vm_settings = self._optional_get(data, 'vm_settings', VMSettings)
            self.cloud_settings = self._optional_get(data, 'cloud_settings', CloudSettings)
            self.job_api_settings = self._optional_get(data, 'job_api', JobApiSettings)

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
            'work_queue': {
                'host': work_queue.host,
                'username': work_queue.worker_username,
                'password': work_queue.worker_password,
                'queue_name': queue_name
            }
        }
        return yaml.safe_dump(data, default_flow_style=False)


class WorkQueue(object):
    """
    Settings for the AMQP used to control the lando Server
    """
    def __init__(self, data):
        self.host = data['host']
        self.username = data.get('username')
        self.password = data.get('password')
        self.worker_username = data.get('worker_username')
        self.worker_password = data.get('worker_password')
        self.listen_queue = data.get('listen_queue')


class VMSettings(object):
    """
    Settings used to create a VM for running a job on.
    """
    def __init__(self, data):
        self.worker_image_name = data['worker_image_name']
        self.ssh_key_name = data['ssh_key_name']
        self.network_name = data['network_name']
        self.floating_ip_pool_name = data['floating_ip_pool_name']
        self.default_favor_name = data['default_favor_name']


class CloudSettings(object):
    """
    Settings used to connect to the VM provider.
    """
    def __init__(self, data):
        self.auth_url = data['auth_url']
        self.username = data['username']
        self.user_domain_name = data['user_domain_name']
        self.project_name = data['project_name']
        self.project_domain_name = data['project_domain_name']
        self.password = data['password']

    def credentials(self):
        """
        Make credentials for connecting to the cloud.
        :return: dict: cloud credentials read from config file
        """
        return {
          'auth_url': self.auth_url,
          'username': self.username,
          'user_domain_name': self.user_domain_name,
          'project_name' : self.project_name,
          'project_domain_name': self.project_domain_name,
          'password': self.password,
        }


class JobApiSettings(object):
    def __init__(self, data):
        self.url = data['url']
        self.username = data['username']
        self.password = data['password']


