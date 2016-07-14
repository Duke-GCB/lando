from time import sleep

from credentials import credentials

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneauth1 import loading
from keystoneclient.v3 import client

import glanceclient

def get_keystone():
    auth = v3.Password(**credentials)
    sess = session.Session(auth=auth)
    keystone = client.Client(session=sess, interface='public')
    return keystone


def image_list(glance):
    print 'listing images'
    for image in glance.images.list():
        yield image


def launch_instance(image):
    print 'launching image {} to instance'.format(image)
    return 'instance of {}'.format(image)


def terminate_instance(instance):
    print 'terminating instance {}'.format(instance)
    pass


def get_glance():
    loader = loading.get_plugin_loader('password')
    auth = loader.load_from_options(**credentials)
    sess = session.Session(auth=auth)
    glance = glanceclient.Client('2', session=sess)
    return glance


def main():
    glance = get_glance()
    image = None
    for i in image_list(glance):
        if 'ubuntu-trusty' in i['name']:
            image = i
    instance = launch_instance(image)
    terminate_instance(instance)


if __name__ == '__main__':
    main()