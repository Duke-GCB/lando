from unittest import TestCase
from unittest.mock import patch, Mock
from lando.k8s.cluster import ClusterApi, AccessModes
from kubernetes import client


class TestClusterApi(TestCase):
    def setUp(self):
        self.cluster_api = ClusterApi(host='somehost', token='myToken', namespace='lando-job-runner', verify_ssl=False)
        self.mock_core_api = Mock()
        self.mock_batch_api = Mock()
        self.cluster_api.core = self.mock_core_api
        self.cluster_api.batch = self.mock_batch_api

    def test_constructor(self):
        configuration = self.cluster_api.api_client.configuration
        self.assertEqual(configuration.host, 'somehost')
        self.assertEqual(configuration.api_key, {"authorization": "Bearer myToken"})
        self.assertEqual(configuration.verify_ssl, False)

    def test_create_persistent_volume_claim(self):
        resp = self.cluster_api.create_persistent_volume_claim(name='myvolume', storage_size_in_g=2,
                                                          storage_class_name='gluster')
        self.assertEqual(resp, self.mock_core_api.create_namespaced_persistent_volume_claim.return_value)
        args, kwargs = self.mock_core_api.create_namespaced_persistent_volume_claim.call_args
        namespace = args[0]
        self.assertEqual(namespace, 'lando-job-runner')
        pvc = args[1]
        self.assertEqual(pvc.metadata.name, 'myvolume')
        self.assertEqual(pvc.spec.access_modes, [AccessModes.READ_WRITE_MANY])
        self.assertEqual(pvc.spec.resources.requests, {'storage': '2Gi'})
        self.assertEqual(pvc.spec.storage_class_name, 'gluster')

    def test_create_persistent_volume_claim_custom_access_mode(self):
        resp = self.cluster_api.create_persistent_volume_claim(name='myvolume', storage_size_in_g=2,
                                                               storage_class_name='gluster',
                                                               access_modes=[AccessModes.READ_WRITE_ONCE])
        self.assertEqual(resp, self.mock_core_api.create_namespaced_persistent_volume_claim.return_value)
        args, kwargs = self.mock_core_api.create_namespaced_persistent_volume_claim.call_args
        pvc = args[1]
        self.assertEqual(pvc.spec.access_modes, [AccessModes.READ_WRITE_ONCE])

    def test_delete_persistent_volume_claim(self):
        self.cluster_api.delete_persistent_volume_claim(name='myvolume')
        self.mock_core_api.delete_namespaced_persistent_volume_claim.assert_called_with(
            'myvolume', 'lando-job-runner', client.V1DeleteOptions()
        )

    def test_create_secret(self):
        resp = self.cluster_api.create_secret(name='mysecret', string_value_dict={
            'password': 's3cr3t'
        })
        self.assertEqual(resp, self.mock_core_api.create_namespaced_secret.return_value)
        args, kwargs = self.mock_core_api.create_namespaced_secret.call_args
        self.assertEqual(kwargs['namespace'], 'lando-job-runner')
        self.assertEqual(kwargs['body'].metadata['name'], 'mysecret')
        self.assertEqual(kwargs['body'].string_data, {'password': 's3cr3t'})

    def test_delete_secret(self):
        self.cluster_api.delete_secret(name='mysecret')
        self.mock_core_api.delete_namespaced_secret.assert_called_with(
            'mysecret', 'lando-job-runner', body=client.V1DeleteOptions()
        )

    def test_create_job(self):
        mock_batch_job_spec = Mock()
        resp = self.cluster_api.create_job(name='myjob', batch_job_spec=mock_batch_job_spec)

        self.assertEqual(resp, self.mock_batch_api.create_namespaced_job.return_value)
        args, kwargs =  self.mock_batch_api.create_namespaced_job.call_args
        self.assertEqual(args[0], 'lando-job-runner')
        self.assertEqual(args[1].metadata.name, 'myjob')
        self.assertEqual(args[1].spec, mock_batch_job_spec.create.return_value)

#    def test_wait_for_job_events(self):
#        self.assertEqual(1, 2)

    def test_delete_job(self):
        self.cluster_api.delete_job(name='myjob')
        args, kwargs = self.mock_batch_api.delete_namespaced_job.call_args
        self.assertEqual(args[0], 'myjob')
        self.assertEqual(args[1], 'lando-job-runner')
        self.assertEqual(kwargs['body'].propagation_policy, 'Background')

    def test_delete_job_custom_propogation_policy(self):
        self.cluster_api.delete_job(name='myjob', propagation_policy='Foreground')
        args, kwargs = self.mock_batch_api.delete_namespaced_job.call_args
        self.assertEqual(kwargs['body'].propagation_policy, 'Foreground')

    def test_create_config_map(self):
        resp = self.cluster_api.create_config_map(name='myconfig', data={'threads': 2})

        self.assertEqual(resp, self.mock_core_api.create_namespaced_config_map.return_value)
        args, kwargs = self.mock_core_api.create_namespaced_config_map.call_args
        self.assertEqual(args[0], 'lando-job-runner')
        self.assertEqual(args[1].metadata.name, 'myconfig')
        self.assertEqual(args[1].data, {'threads': 2})

    def test_delete_config_map(self):
        self.cluster_api.delete_config_map(name='myconfig')
        args, kwargs = self.mock_core_api.delete_namespaced_config_map.call_args
        self.assertEqual(args[0], 'myconfig')
        self.assertEqual(args[1], 'lando-job-runner')

    def read_pod_logs(self, name):
        resp = self.cluster_api.read_pod_logs('mypod')
        self.assertEqual(resp, self.mock_core_api.read_namespaced_pod_log.return_value)
        self.mock_core_api.read_namespaced_pod_log.assert_called_with('mypod', 'lando-job-runner')
