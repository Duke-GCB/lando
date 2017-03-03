from __future__ import absolute_import
from unittest import TestCase
import novaclient.exceptions
from lando.server.cloudservice import CloudService, NovaClient
from lando.exceptions import QuotaExceededException
import mock


class TestCwlWorkflow(TestCase):
    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_that_flavor_overrides_default(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.launch_instance(server_name="worker1", flavor_name='m1.GIANT', script_contents="")
        find_flavor_method = mock_client().flavors.find
        find_flavor_method.assert_called_with(name='m1.GIANT')

    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_that_no_flavor_chooses_default(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.launch_instance(server_name="worker1", flavor_name=None, script_contents="")
        find_flavor_method = mock_client().flavors.find
        find_flavor_method.assert_called_with(name='m1.xbig')

    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_launch_instance_no_floating_ip(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = False
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        instance, ip_address = cloud_service.launch_instance(server_name="worker1", flavor_name=None, script_contents="")
        self.assertEqual(None, ip_address)
        mock_client().servers.create.assert_called()
        mock_client().floating_ips.create.assert_not_called()

    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_launch_instance_with_floating_ip(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = True
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        instance, ip_address = cloud_service.launch_instance(server_name="worker1", flavor_name=None,
                                                             script_contents="")
        self.assertNotEqual(None, ip_address)
        mock_client().servers.create.assert_called()
        mock_client().floating_ips.create.assert_called()

    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_terminate_instance_no_floating_ip(self, mock_client, mock_session, mock_get_plugin_loader):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = False
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.terminate_instance(server_name='worker1')
        mock_client().servers.delete.assert_called()
        mock_client().floating_ips.find.assert_not_called()

    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_terminate_instance_with_floating_ip(self, mock_client, mock_session, mock_get_plugin_loader):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = True
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.terminate_instance(server_name='worker1')
        mock_client().servers.delete.assert_called()
        mock_client().floating_ips.find.assert_called()
        mock_client().floating_ips.find().delete.assert_called()

    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_terminate_instance_with_missing_floating_ip(self, mock_client, mock_session, mock_get_plugin_loader):
        mock_client().floating_ips.find.side_effect = novaclient.exceptions.NotFound(404)
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = True
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.terminate_instance(server_name='worker1')
        mock_client().servers.delete.assert_called()
        mock_client().floating_ips.find.assert_called()

    def test_is_quota_exceeded_message(self):
        good_values = [
            "Quota exceeded for cores",
            "Quota exceeded for resources"
        ]
        bad_values = [
            "Invalid image format",
            "Security group not found"
        ]
        for val in good_values:
            self.assertEqual(True, NovaClient.is_quota_exceeded_message(val))
        for val in bad_values:
            self.assertEqual(False, NovaClient.is_quota_exceeded_message(val))

    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_launch_instance_quota_exceeded(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        mock_client().servers.create.side_effect = novaclient.exceptions.ClientException(
            code=403,
            message="Quota exceeded for cores")
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        with self.assertRaises(QuotaExceededException):
            cloud_service.launch_instance(server_name="worker1", flavor_name='m1.GIANT', script_contents="")

    @mock.patch('lando.server.cloudservice.sleep')
    @mock.patch('keystoneauth1.loading.get_plugin_loader')
    @mock.patch('keystoneauth1.session.Session')
    @mock.patch('novaclient.client.Client')
    def test_launch_instance_invalid_image_format(self, mock_client, mock_session, mock_get_plugin_loader, mock_sleep):
        mock_client().servers.create.side_effect = novaclient.exceptions.ClientException(
            code=403,
            message="Invalid image format")
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        with self.assertRaises(novaclient.exceptions.ClientException):
            cloud_service.launch_instance(server_name="worker1", flavor_name='m1.GIANT', script_contents="")
