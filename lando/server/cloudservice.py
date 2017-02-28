"""
Allows launching and terminating openstack virtual machines.
"""
from keystoneauth1 import session
from keystoneauth1 import loading
import novaclient.exceptions
from novaclient import client as nvclient
from time import sleep
import logging
import uuid

WAIT_BEFORE_ATTACHING_IP = 5


class NovaClient(object):
    """
    Wraps up openstack nova operations.
    """
    def __init__(self, credentials):
        """
        Setup internal nova client based on credentials in cloud_settings
        :credentials: dictionary of url, username, password, etc
        """
        nova_session = NovaClient.get_session(credentials)
        self.nova = nvclient.Client('2', session=nova_session)

    @staticmethod
    def get_session(credentials):
        """
        Returns a session from openstack credentials. Used by nova client
        :credentials: dictionary of url, username, password, etc
        :return: session.Session for the specified credentials
        """
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(**credentials)
        sess = session.Session(auth=auth)
        return sess

    def launch_instance(self, vm_settings, server_name, flavor_name, script_contents):
        """
        Start VM with the specified settings, name, and script to run on startup.
        :param vm_settings: config.VMSettings: settings for VM we want to create
        :param server_name: str: unique name for this VM
        :param flavor_name: str: name of flavor(RAM/CPUs) to use for the VM (None uses config.vm_settings.default_favor_name)
        :param script_contents: str: contents of a bash script that will be run on startup
        :return: openstack instance created
        """
        image = self.nova.images.find(name=vm_settings.worker_image_name)
        vm_flavor_name = flavor_name
        if not vm_flavor_name:
            vm_flavor_name = vm_settings.default_favor_name
        flavor = self.nova.flavors.find(name=vm_flavor_name)
        net = self.nova.networks.find(label=vm_settings.network_name)
        nics = [{'net-id': net.id}]
        instance = self.nova.servers.create(name=server_name, image=image, flavor=flavor,
                                            key_name=vm_settings.ssh_key_name,
                                            nics=nics, userdata=script_contents)
        return instance

    def attach_floating_ip(self, instance, floating_ip_pool_name):
        """
        Attach a floating IP address to an openstack VM.
        :param instance: openstack VM
        :param floating_ip_pool_name: str: name of the pool of ip addresses
        :return: str: ip address that was assigned
        """
        floating_ip = self.nova.floating_ips.create(floating_ip_pool_name)
        instance.add_floating_ip(floating_ip)
        return floating_ip.ip

    def _delete_floating_ip(self, instance):
        """
        Delete floating ip address associated with the instance with server_name.
        If not found will log as as warning.
        :param instance: Server: VM we want to delete a floating IP from
        """
        try:
            floating_ip = self.nova.floating_ips.find(instance_id=instance.id)
            floating_ip.delete()
        except novaclient.exceptions.NotFound:
            logging.warn('No floating ip address found for {} ({})'.format(instance.name, instance.id))

    def terminate_instance(self, server_name, delete_floating_ip):
        """
        Terminate a VM based on name.
        :param server_name: str: name of the VM to terminate
        :param delete_floating_ip: bool: should we try to delete an attached floating ip address
        """
        instance = self.nova.servers.find(name=server_name)
        if delete_floating_ip:
            self._delete_floating_ip(instance)
        self.nova.servers.delete(instance)


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
        self.nova_client = NovaClient(config.cloud_settings.credentials(project_name))

    def launch_instance(self, server_name, flavor_name, script_contents):
        """
        Start a new VM with the specified name and script to run on start.
        :param server_name: str: unique name for the server.
        :param flavor_name: str: name of flavor(RAM/CPUs) to use for the VM (None uses config.vm_settings.default_favor_name)
        :param script_contents: str: bash script to be run when VM starts.
        :return: instance, ip address: openstack instance object and the floating ip address assigned
        """
        vm_settings = self.config.vm_settings
        instance = self.nova_client.launch_instance(vm_settings, server_name, flavor_name, script_contents)
        ip_address = None
        if vm_settings.allocate_floating_ips:
            sleep(WAIT_BEFORE_ATTACHING_IP)
            ip_address = self.nova_client.attach_floating_ip(instance, vm_settings.floating_ip_pool_name)
            logging.info('launched {} on floating ip {}'.format(server_name, ip_address))
        else:
            logging.info('launched {} with id'.format(server_name, instance.id))
        return instance, ip_address

    def terminate_instance(self, server_name):
        """
        Terminate the VM with server_name and deletes attached floating ip address
        :param server_name: str: name of the VM to terminate
        """
        vm_settings = self.config.vm_settings
        logging.info('terminating instance {}'.format(server_name))
        self.nova_client.terminate_instance(server_name, delete_floating_ip=vm_settings.allocate_floating_ips)

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
