"""
Allows launching and terminating openstack virtual machines.
"""
import shade
import logging
import uuid


class CloudClient(object):
    """
    Wraps up openstack shade operations.
    """
    def __init__(self, credentials):
        """
        Setup internal client based on credentials in cloud_settings
        :credentials: dictionary of url, username, password, etc
        """
        self.cloud = shade.openstack_cloud(**credentials)

    def launch_instance(self, vm_settings, server_name, flavor_name, script_contents):
        """
        Start VM with the specified settings, name, and script to run on startup.
        :param vm_settings: config.VMSettings: settings for VM we want to create
        :param server_name: str: unique name for this VM
        :param flavor_name: str: name of flavor(RAM/CPUs) to use for the VM (None uses config.vm_settings.default_favor_name)
        :param script_contents: str: contents of a bash script that will be run on startup
        :return: openstack instance created
        """
        vm_flavor_name = flavor_name
        if not vm_flavor_name:
            vm_flavor_name = vm_settings.default_favor_name
        instance = self.cloud.create_server(
            name=server_name,
            image=vm_settings.worker_image_name,
            flavor=vm_flavor_name,
            key_name=vm_settings.ssh_key_name,
            network=vm_settings.network_name,
            auto_ip=vm_settings.allocate_floating_ips,
            ip_pool=vm_settings.floating_ip_pool_name,
            userdata=script_contents)
        return instance

    def terminate_instance(self, server_name, delete_floating_ip):
        """
        Terminate a VM based on name.
        :param server_name: str: name of the VM to terminate
        :param delete_floating_ip: bool: should we try to delete an attached floating ip address
        """
        self.cloud.delete_server(server_name, delete_ips=delete_floating_ip)


class CloudService(object):
    """
    Service for creating and terminating virtual machines.
    """
    def __init__(self, config, project_name):
        """
        Setup configuration needed to connect to cloud service and
        :param config: Config config settings for vm and credentials
        :param project_name: name of the project(tenant) which will contain our VMs
        """
        self.config = config
        self.cloud_client = CloudClient(config.cloud_settings.credentials(project_name))

    def launch_instance(self, server_name, flavor_name, script_contents):
        """
        Start a new VM with the specified name and script to run on start.
        :param server_name: str: unique name for the server.
        :param flavor_name: str: name of flavor(RAM/CPUs) to use for the VM (None uses config.vm_settings.default_favor_name)
        :param script_contents: str: bash script to be run when VM starts.
        :return: instance, ip address: openstack instance object and the floating ip address assigned
        """
        vm_settings = self.config.vm_settings
        instance = self.cloud_client.launch_instance(vm_settings, server_name, flavor_name, script_contents)
        return instance, instance.accessIPv4

    def terminate_instance(self, server_name):
        """
        Terminate the VM with server_name and deletes attached floating ip address
        :param server_name: str: name of the VM to terminate
        """
        vm_settings = self.config.vm_settings
        logging.info('terminating instance {}'.format(server_name))
        self.cloud_client.terminate_instance(server_name, delete_floating_ip=vm_settings.allocate_floating_ips)

    def make_vm_name(self, job_id):
        """
        Create a unique vm name for this job id
        :param job_id: int: unique job id
        :return: str
        """
        return 'job{}_{}'.format(job_id, uuid.uuid4())


class FakeCloudService(object):
    """
    Fake cloud service so lando/lando_worker can be run locally.
    """
    def __init__(self, config):
        self.config = config

    def launch_instance(self, server_name, flavor_name, script_contents):
        print("Pretend we create vm: {}".format(server_name))
        return None, '127.0.0.1'

    def terminate_instance(self, server_name):
        print("Pretend we terminate: {}".format(server_name))

    def make_vm_name(self, job_id):
        return 'local_worker'
