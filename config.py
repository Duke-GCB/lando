"""
Reads configuration settings from a YAML file for use with lando.py and lando_client.py.
"""
import yaml

WORK_QUEUE_CONFIG_NAME = ''


class Config(object):
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
            self.data = yaml.load(infile)

    def work_queue_config(self):
        """
        Return work queue information from yaml loaded in constructor
        :return: WorkQueue
        """
        return WorkQueue(self.data['work_queue'])

    def vm_settings(self):
        """
        Return virtual machine settings or None based on yaml loaded in constructor
        :return: VMSettings
        """
        return self._optional_get('vm_settings', VMSettings)

    def cloud_settings(self):
        """
        Return cloud service settings or None based on yaml loaded in constructor
        :return: CloudSettings
        """
        return self._optional_get('cloud_settings', CloudSettings)

    def _optional_get(self, name, constructor):
        data = self.data.get(name, None)
        if data:
            return constructor(data)
        else:
            return None

    def make_worker_config_yml(self):
        """
        Create a worker config file that can be sent to a worker VM so they can talk to the work queue.
        :return: str: worker config file
        """
        work_queue = self.work_queue_config()
        data = {
            'work_queue': {
                'host': work_queue.host,
                'username': work_queue.worker_username,
                'password': work_queue.worker_password,
                'queue_name': work_queue.queue_name,
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
        self.queue_name = data['queue_name']


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
