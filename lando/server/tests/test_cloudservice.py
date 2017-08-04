from __future__ import absolute_import
from unittest import TestCase
from lando.server.cloudservice import CloudService
import mock


class TestCloudService(TestCase):
    @mock.patch('lando.server.cloudservice.shade')
    def test_that_flavor_overrides_default(self, mock_shade):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.launch_instance(server_name="worker1", flavor_name='m1.GIANT', script_contents="",
                                      volumes=['volume1'])
        mock_shade.openstack_cloud()
        mock_shade.openstack_cloud().create_server.assert_called()
        args, kw_args = mock_shade.openstack_cloud().create_server.call_args
        self.assertEqual(kw_args['flavor'], 'm1.GIANT')

    @mock.patch('lando.server.cloudservice.shade')
    def test_that_no_flavor_chooses_default(self, mock_shade):
        config = mock.MagicMock()
        config.vm_settings.default_favor_name = 'm1.xbig'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.launch_instance(server_name="worker1", flavor_name=None, script_contents="",volumes=['volume1'])
        mock_shade.openstack_cloud().create_server.assert_called()
        args, kw_args = mock_shade.openstack_cloud().create_server.call_args
        self.assertEqual(kw_args['flavor'], 'm1.xbig')
        self.assertEqual(kw_args['volumes'], ['volume1'])

    @mock.patch('lando.server.cloudservice.shade')
    def test_launch_instance_no_floating_ip(self, mock_shade):
        mock_shade.openstack_cloud().create_server.return_value = mock.Mock(accessIPv4='')
        config = mock.MagicMock(vm_settings=mock.Mock(worker_image_name='myvm', floating_ip_pool_name='somepool'))
        config.vm_settings.allocate_floating_ips = False
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        instance, ip_address = cloud_service.launch_instance(server_name="worker1", flavor_name=None,
                                                             script_contents="", volumes=['volume1'])
        self.assertEqual('', ip_address)
        mock_shade.openstack_cloud().create_server.assert_called()
        args, kw_args = mock_shade.openstack_cloud().create_server.call_args
        self.assertEqual(kw_args['auto_ip'], False)
        self.assertEqual(kw_args['volumes'], ['volume1'])

    @mock.patch('lando.server.cloudservice.shade')
    def test_launch_instance_with_floating_ip(self, mock_shade):
        mock_shade.openstack_cloud().create_server.return_value = mock.Mock(accessIPv4='123')
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = True
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        instance, ip_address = cloud_service.launch_instance(server_name="worker1", flavor_name=None,
                                                             script_contents="", volumes=['volume1'])
        self.assertNotEqual(None, ip_address)
        mock_shade.openstack_cloud().create_server.assert_called()
        args, kw_args = mock_shade.openstack_cloud().create_server.call_args
        self.assertEqual(kw_args['auto_ip'], True)

    @mock.patch('lando.server.cloudservice.shade')
    def test_terminate_instance_no_floating_ip(self, mock_shade):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = False
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.terminate_instance(server_name='worker1', volume_names=[])
        mock_shade.openstack_cloud().delete_server.assert_called()
        args, kw_args = mock_shade.openstack_cloud().delete_server.call_args
        self.assertEqual(kw_args['delete_ips'], False)

    @mock.patch('lando.server.cloudservice.shade')
    def test_terminate_instance_with_floating_ip(self, mock_shade):
        config = mock.MagicMock()
        config.vm_settings.allocate_floating_ips = True
        config.vm_settings.default_favor_name = 'm1.large'
        cloud_service = CloudService(config, project_name='bespin_user1')
        cloud_service.terminate_instance(server_name='worker1')
        args, kw_args = mock_shade.openstack_cloud().delete_server.call_args
        self.assertEqual(kw_args['delete_ips'], True)

    def test_create_volume(self):
        self.fail('Not yet implemented')

    def test_terminate_instance_with_volumes(self):
        self.fail('Not yet implemented')

    def test_make_volume_name(self):
        self.fail('Not yet implemented')

