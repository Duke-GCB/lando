from keystoneauth1 import session
from keystoneauth1 import loading
from novaclient import client as nvclient
from time import sleep
import logging

WAIT_BEFORE_ATTACHING_IP = 5


class NovaClient(object):
    def __init__(self, cloud_settings):
        nova_session = NovaClient.get_session(cloud_settings.credentials())
        self.nova = nvclient.Client('2', session=nova_session)

    @staticmethod
    def get_session(credentials):
        """
        Returns a session from openstack credentials. Used by nova client
        :return:
        """
        loader = loading.get_plugin_loader('password')

        auth = loader.load_from_options(**credentials)
        sess = session.Session(auth=auth)
        return sess

    def launch_instance(self, vm_settings, server_name, script_contents):
        image = self.nova.images.find(name=vm_settings.worker_image_name)
        flavor = self.nova.flavors.find(name=vm_settings.default_favor_name)
        net = self.nova.networks.find(label=vm_settings.network_name)
        nics = [{'net-id': net.id}]
        instance = self.nova.servers.create(name=server_name, image=image, flavor=flavor,
                                            key_name=vm_settings.ssh_key_name,
                                            nics=nics, userdata=script_contents)
        return instance

    def attach_floating_ip(self, instance, floating_ip_pool_name):
        floating_ip = self.nova.floating_ips.create(floating_ip_pool_name)
        instance.add_floating_ip(floating_ip)
        return floating_ip.ip

    def terminate_instance(self, server_name):
        s = self.nova.servers.find(name=server_name)
        self.nova.servers.delete(s)


class CloudService(object):
    """
    Service for creating and terminating virtual machines.
    """
    def __init__(self, config):
        """
        Setup configuration needed to connect to cloud service and
        :param config: Config config settings for vm and credentials
        """
        self.config = config
        self.nova_client = NovaClient(config.cloud_settings())

    def launch_instance(self, server_name, script_contents):
        vm_settings = self.config.vm_settings()
        instance = self.nova_client.launch_instance(vm_settings, server_name, script_contents)
        sleep(WAIT_BEFORE_ATTACHING_IP)
        ip_address = self.nova_client.attach_floating_ip(instance, vm_settings.floating_ip_pool_name)
        logging.info('launched {} on ip {}'.format(server_name, ip_address))
        return instance, ip_address

    def terminate_instance(self, server_name):
        logging.info('terminating instance {}'.format(server_name))
        self.nova_client.terminate_instance(server_name)
