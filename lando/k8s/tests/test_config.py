from unittest import TestCase
from lando.k8s.config import create_server_config, InvalidConfigException, ServerConfig, DEFAULT_REQUESTED_MEMORY, \
    DEFAULT_REQUESTED_CPU
from unittest.mock import patch
import logging

MINIMAL_CONFIG = {
    'work_queue': {
        'host': 'somehost',
        'username': 'lando',
        'password': 'secret',
        'worker_username': 'worker',
        'worker_password': 'secret2',
        'listen_queue': 'lando'
    },
    'cluster_api_settings': {
        'host': 'somehost2',
        'token': 'myToken1',
        'namespace': 'lando-job-runner',
    },
    'bespin_api': {
        'url': 'someurl',
        'token': 'myToken2',
    },
    'data_store_settings': {
        'secret_name': 'ddsclient-secret'
    },
    'stage_data_settings': {
        'image_name': 'lando-util:1',
        'command': 'download.py',
    },
    'run_workflow_settings': {},
    'organize_output_settings': {
        'image_name': 'lando-util:2',
        'command': 'organize.py',
    },
    'save_output_settings': {
        'image_name': 'lando-util:3',
        'command': 'upload.py',
    },
    'record_output_project_settings': {
        'image_name': 'lachlanevenson/k8s-kubectl',
        'service_account_name': 'annotation-writer-sa',
    },
}

FULL_CONFIG = {
    'log_level': logging.DEBUG,
    'work_queue': {
        'host': 'somehost',
        'username': 'lando',
        'password': 'secret',
        'worker_username': 'worker',
        'worker_password': 'secret2',
        'listen_queue': 'lando'
    },
    'cluster_api_settings': {
        'host': 'somehost2',
        'token': 'myToken1',
        'namespace': 'lando-job-runner',
        'verify_ssl': False,
    },
    'bespin_api': {
        'url': 'someurl',
        'token': 'myToken2',
    },
    'data_store_settings': {
        'secret_name': 'ddsclient-secret'
    },
    'stage_data_settings': {
        'image_name': 'lando-util:1',
        'command': 'download.py',
        'requested_cpu': 2,
        'requested_memory': '2G',
    },
    'run_workflow_settings': {
        'requested_cpu': 3,
        'requested_memory': '3G',
        'system_data_volume': {
            'mount_path': '/system/data',
            'volume_claim_name': 'system-data',
        }
    },
    'organize_output_settings': {
        'image_name': 'lando-util:2',
        'command': 'organize.py',
        'requested_cpu': 4,
        'requested_memory': '4G',
    },
    'save_output_settings': {
        'image_name': 'lando-util:3',
        'command': 'upload.py',
        'requested_cpu': 5,
        'requested_memory': '5G',
    },
    'record_output_project_settings': {
        'image_name': 'lachlanevenson/k8s-kubectl',
        'service_account_name': 'annotation-writer-sa',
    },
    'storage_class_name': 'gluster'
}


class TestServerConfig(TestCase):
    @patch('lando.k8s.config.ServerConfig')
    @patch('builtins.open')
    @patch('lando.k8s.config.yaml')
    def test_create_server_config(self, mock_yaml, mock_open, mock_server_config):
        mock_yaml.load.return_value = {"logging": "INFO"}
        server_config = create_server_config('somefile')
        self.assertEqual(server_config, mock_server_config.return_value)
        mock_server_config.assert_called_with({"logging": "INFO"})

        mock_yaml.load.return_value = {}
        with self.assertRaises(InvalidConfigException):
            create_server_config('somefile')

    def test_minimal_config(self):
        config = ServerConfig(MINIMAL_CONFIG)
        self.assertEqual(config.log_level, logging.WARN)
        self.assertIsNotNone(config.work_queue_config)

        self.assertEqual(config.cluster_api_settings.host, 'somehost2')
        self.assertEqual(config.cluster_api_settings.token, 'myToken1')
        self.assertEqual(config.cluster_api_settings.namespace, 'lando-job-runner')
        self.assertEqual(config.cluster_api_settings.verify_ssl, True)

        self.assertIsNotNone(config.bespin_api_settings)

        self.assertEqual(config.data_store_settings.secret_name, 'ddsclient-secret')

        self.assertEqual(config.stage_data_settings.image_name, 'lando-util:1')
        self.assertEqual(config.stage_data_settings.command, 'download.py')
        self.assertEqual(config.stage_data_settings.requested_cpu, DEFAULT_REQUESTED_CPU)
        self.assertEqual(config.stage_data_settings.requested_memory, DEFAULT_REQUESTED_MEMORY)

        self.assertEqual(config.run_workflow_settings.requested_cpu, DEFAULT_REQUESTED_CPU)
        self.assertEqual(config.run_workflow_settings.requested_memory, DEFAULT_REQUESTED_MEMORY)
        self.assertEqual(config.run_workflow_settings.system_data_volume, None)

        self.assertEqual(config.organize_output_settings.image_name, 'lando-util:2')
        self.assertEqual(config.organize_output_settings.command, 'organize.py')
        self.assertEqual(config.organize_output_settings.requested_cpu, DEFAULT_REQUESTED_CPU)
        self.assertEqual(config.organize_output_settings.requested_memory, DEFAULT_REQUESTED_MEMORY)

        self.assertEqual(config.save_output_settings.image_name, 'lando-util:3')
        self.assertEqual(config.save_output_settings.command, 'upload.py')
        self.assertEqual(config.save_output_settings.requested_cpu, DEFAULT_REQUESTED_CPU)
        self.assertEqual(config.save_output_settings.requested_memory, DEFAULT_REQUESTED_MEMORY)

        self.assertEqual(config.record_output_project_settings.image_name, 'lachlanevenson/k8s-kubectl')
        self.assertEqual(config.record_output_project_settings.service_account_name, 'annotation-writer-sa')

        self.assertEqual(config.storage_class_name, None)

    def test_optional_config(self):
        config = ServerConfig(FULL_CONFIG)
        self.assertEqual(config.log_level, logging.DEBUG)
        self.assertEqual(config.stage_data_settings.requested_cpu, 2)
        self.assertEqual(config.stage_data_settings.requested_memory, '2G')
        self.assertEqual(config.run_workflow_settings.requested_cpu, 3)
        self.assertEqual(config.run_workflow_settings.requested_memory, '3G')
        self.assertEqual(config.run_workflow_settings.system_data_volume.mount_path, '/system/data')
        self.assertEqual(config.run_workflow_settings.system_data_volume.volume_claim_name, 'system-data')
        self.assertEqual(config.organize_output_settings.requested_cpu, 4)
        self.assertEqual(config.organize_output_settings.requested_memory, '4G')
        self.assertEqual(config.save_output_settings.requested_cpu, 5)
        self.assertEqual(config.save_output_settings.requested_memory, '5G')
        self.assertEqual(config.cluster_api_settings.verify_ssl, False)
