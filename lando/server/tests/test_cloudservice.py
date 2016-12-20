from __future__ import absolute_import
from unittest import TestCase
from lando.server.cloudservice import CloudService
import mock


class TestCwlWorkflow(TestCase):
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_that_flavor_overrides_default(self, mock_client, mock_session, mock_get_plugin_loader):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config)
        cloud_service.launch_instance(server_name="worker1", flavor_name='m1.GIANT', script_contents="")
        find_flavor_method = mock_client().flavors.find
        find_flavor_method.assert_called_with(name='m1.GIANT')

    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_that_no_flavor_chooses_default(self, mock_client, mock_session, mock_get_plugin_loader):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config)
        cloud_service.launch_instance(server_name="worker1", flavor_name=None, script_contents="")
        find_flavor_method = mock_client().flavors.find
        find_flavor_method.assert_called_with(name='m1.xbig')



