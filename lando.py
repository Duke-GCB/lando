#!/usr/bin/env python

# Simple script to interact with openstack, launch an instance, and delete it
# I thought I needed to go through keystone, then glance, then nova
# but the nova client has facilities to list images and can authenticate directly
# with some keystone helpers. Pretty simple

SERVER_NAME='lando-test'
IMAGE_NAME='ubuntu-trusty'
PUBLIC_KEY_NAME='dcl9'
NETWORK_NAME='selfservice'
FLAVOR_NAME='m1.small'
FLOATING_IP_POOL_NAME='ext-net'

from time import sleep

from credentials import credentials

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneauth1 import loading
from keystoneclient.v3 import client

import glanceclient


from novaclient import client as nvclient

def get_keystone():
    """
    creates a keystone client
    :return:
    """
    auth = v3.Password(**credentials)
    sess = session.Session(auth=auth)
    keystone = client.Client(session=sess, interface='public')
    return keystone


def get_session():
    """
    Returns a session from openstack credentials. Used by glance/nova clients
    :return:
    """
    loader = loading.get_plugin_loader('password')
    auth = loader.load_from_options(**credentials)
    sess = session.Session(auth=auth)
    return sess


def get_glance():
    """
    Creates a glance client for managing images
    :return:
    """
    return glanceclient.Client('2', session=get_session())


def get_nova():
    """
    Creates a nova client for launching/terminating servers
    :return:
    """
    return nvclient.Client('2', session=get_session())


def image_list(glance):
    for image in glance.images.list():
        print image


def launch_instance():
    # http://docs.openstack.org/user-guide/sdk_compute_apis.html
    nova = get_nova()
    image = nova.images.find(name=IMAGE_NAME)
    flavor = nova.flavors.find(name=FLAVOR_NAME)
    net = nova.networks.find(label=NETWORK_NAME)

    nics = [{'net-id': net.id}]
    instance = nova.servers.create(name=SERVER_NAME, image=image, flavor=flavor, key_name=PUBLIC_KEY_NAME, nics=nics)
    # Creates a floating IP in the ext
    floating_ip =  nova.floating_ips.create(FLOATING_IP_POOL_NAME)
    sleep(5)
    instance.add_floating_ip(floating_ip)
    print 'launched instance {} with key {}'.format(instance.name, PUBLIC_KEY_NAME)
    print 'Try to ssh ubuntu@{}'.format(floating_ip.ip)
    return instance


def terminate_instance():
    nova = get_nova()
    print 'terminating instance {}'.format(SERVER_NAME)
    s = nova.servers.find(name=SERVER_NAME)
    nova.servers.delete(s)


def main():
    launch_instance()
    raw_input("Press Enter to terminate {}...".format(SERVER_NAME))
    terminate_instance()


if __name__ == '__main__':
    main()