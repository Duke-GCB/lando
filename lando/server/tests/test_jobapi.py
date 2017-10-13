from __future__ import absolute_import
from unittest import TestCase
from lando.server.jobapi import JobApi, BespinApi, Job
from mock.mock import MagicMock, patch, call


class TestJobApi(TestCase):
    def setUp(self):
        self.job_response_payload = {
            'id': 1,
            'user': {
                'id': 23,
                'username': 'joe@joe.com'
            },
            'state': 'N',
            'step': '',
            'name': 'myjob',
            'created': '2017-03-21T13:29:09.123603Z',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            'vm_volume_name': '',
            "vm_project_name": 'jpb67',
            'job_order': '{ "value": 1 }',
            'workflow_version': {
                'name': 'SomeWorkflow',
                'version': 1,
                'url': 'file:///mnt/fastqc.cwl',
                'object_name': '#main',
                "methods_document": 7,
            },
            'output_project': {
                'id': 5,
                'dds_user_credentials': 123
            },
            'stage_group': None,
            'share_group': 42,
            'volume_size': 100,
            'cleanup_vm': True,
        }

    def setup_job_api(self, job_id):
        def empty_headers():
            return {}
        mock_config = MagicMock()
        mock_config.bespin_api_settings.url = 'APIURL'
        job_api = JobApi(mock_config, job_id)
        job_api.api.headers = empty_headers
        return job_api

    @patch('lando.server.jobapi.requests')
    def test_get_job_api(self, mock_requests):
        """
        Test requesting job status, etc
        """
        job_api = self.setup_job_api(1)

        mock_response = MagicMock()
        mock_response.json.return_value = self.job_response_payload
        mock_requests.get.return_value = mock_response
        job = job_api.get_job()
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/1/')

        self.assertEqual(1, job.id)
        self.assertEqual(23, job.user_id)
        self.assertEqual('joe@joe.com', job.username)
        self.assertEqual('N', job.state)
        self.assertEqual('m1.tiny', job.vm_flavor)
        self.assertEqual('', job.vm_instance_name)
        self.assertEqual('', job.vm_volume_name)
        self.assertEqual(True, job.cleanup_vm)
        self.assertEqual('jpb67', job.vm_project_name)

        self.assertEqual('{ "value": 1 }', job.workflow.job_order)
        self.assertEqual('file:///mnt/fastqc.cwl', job.workflow.url)
        self.assertEqual('#main', job.workflow.object_name)

    @patch('lando.server.jobapi.requests')
    def test_set_job_state(self, mock_requests):
        job_api = self.setup_job_api(2)
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests.put.return_value = mock_response
        job_api.set_job_state('E')
        args, kwargs = mock_requests.put.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/2/')
        self.assertEqual(kwargs.get('json'), {'state': 'E'})

    @patch('lando.server.jobapi.requests')
    def test_set_job_step(self, mock_requests):
        job_api = self.setup_job_api(2)
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests.put.return_value = mock_response
        job_api.set_job_step('N')
        args, kwargs = mock_requests.put.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/2/')
        self.assertEqual(kwargs.get('json'), {'step': 'N'})

    @patch('lando.server.jobapi.requests')
    def test_set_vm_instance_name(self, mock_requests):
        job_api = self.setup_job_api(3)
        mock_response = MagicMock()
        mock_response.json.return_value = mock_response
        mock_requests.put.return_value = mock_response
        job_api.set_vm_instance_name('worker_123')
        args, kwargs = mock_requests.put.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/3/')
        self.assertEqual(kwargs.get('json'), {'vm_instance_name': 'worker_123'})

    @patch('lando.server.jobapi.requests')
    def test_set_vm_volume_name(self, mock_requests):
        job_api = self.setup_job_api(3)
        mock_response = MagicMock()
        mock_response.json.return_value = mock_response
        mock_requests.put.return_value = mock_response
        job_api.set_vm_volume_name('volume_765')
        args, kwargs = mock_requests.put.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/3/')
        self.assertEqual(kwargs.get('json'), {'vm_volume_name': 'volume_765'})

    @patch('lando.server.jobapi.requests')
    def test_get_input_files(self, mock_requests):
        self.job_response_payload['stage_group'] = '4'
        stage_group_response_payload = {
                'dds_files': [
                    {
                        'file_id': 123,
                        'destination_path': 'seq1.fasta',
                        'dds_user_credentials': 823,
                    }
                ],
                'url_files': [
                    {
                        'url': "https://stuff.com/file123.model",
                        'destination_path': "file123.model",
                    }
                ],
        }
        get_job_response = MagicMock()
        get_job_response.json.return_value = self.job_response_payload
        stage_group_response = MagicMock()
        stage_group_response.json.return_value = stage_group_response_payload
        mock_requests.get.side_effect = [
            get_job_response,
            stage_group_response
        ]
        job_api = self.setup_job_api(4)
        files = job_api.get_input_files()
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/job-file-stage-groups/4')

        self.assertEqual(1, len(files))
        file = files[0]
        self.assertEqual(1, len(file.dds_files))
        dds_file = file.dds_files[0]
        self.assertEqual(123, dds_file.file_id)
        self.assertEqual('seq1.fasta', dds_file.destination_path)
        self.assertEqual(823, dds_file.user_id)
        self.assertEqual(1, len(file.url_files))
        url_file = file.url_files[0]
        self.assertEqual('https://stuff.com/file123.model', url_file.url)
        self.assertEqual('file123.model', url_file.destination_path)

    @patch('lando.server.jobapi.requests')
    def test_get_credentials(self, mock_requests):
        """
        The only stored credentials are the bespin system credentials.
        """
        job_response_payload = {
            'id': 4,
            'user': {
                'id': 1,
                'username': 'joe@joe.com'
            },
            'state': 'N',
            'step': '',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            'vm_volume_name': '',
            "vm_project_name": 'jpb67',
            'name': 'myjob',
            'created': '2017-03-21T13:29:09.123603Z',
            'job_order': '{ "value": 1 }',
            'workflow_version': {
                'url': 'file:///mnt/fastqc.cwl',
                'object_name': '#main',
                'name': 'SomeWorkflow',
                'version': 1,
                "methods_document": 7,
            },
            'output_project': {
                'id': 5,
                'dds_user_credentials': 123
            },
            'stage_group': None,
            'volume_size': 200,
        }
        user_credentials_response = [
            {
                'id': 5,
                'user': 1,
                'token': '1239109',
                'endpoint': {
                    'id': 3,
                    'name': 'dukeds',
                    'agent_key': '2191230',
                    'api_root': 'localhost/api/v1/',
                }
            }
        ]
        mock_response = MagicMock()
        mock_response.json.side_effect = [job_response_payload, user_credentials_response]
        mock_requests.get.return_value = mock_response
        job_api = self.setup_job_api(4)

        user_credentials = job_api.get_credentials()
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/dds-user-credentials/')

        user_cred = user_credentials.dds_user_credentials[5]
        self.assertEqual('1239109', user_cred.token)
        self.assertEqual('2191230', user_cred.endpoint_agent_key)
        self.assertEqual('localhost/api/v1/', user_cred.endpoint_api_root)

    @patch('lando.server.jobapi.requests')
    def test_get_jobs_for_vm_instance_name(self, mock_requests):
        jobs_response = [
            {
                'id': 1,
                'user': {
                    'id': 1,
                    'username': 'joe@joe.com'
                },
                'state': 'N',
                'step': '',
                'name': 'SomeJob',
                'created': '2017-03-21T13:29:09.123603Z',
                'vm_flavor': 'm1.tiny',
                'vm_instance_name': '',
                'vm_volume_name': '',
                "vm_project_name": 'jpb67',
                'job_order': '{ "value": 1 }',
                'workflow_version': {
                    'url': 'file:///mnt/fastqc.cwl',
                    'object_name': '#main',
                    'name': 'myworkflow',
                    'version': 1,
                    "methods_document": 7,
                },
                'output_project': {
                    'id': 5,
                    'dds_user_credentials': 123
                },
                'stage_group': None,
                'volume_size': 200,
            }
        ]

        mock_config = MagicMock()
        mock_config.bespin_api_settings.url = 'APIURL'

        mock_response = MagicMock()
        mock_response.json.side_effect = [jobs_response]
        mock_requests.get.return_value = mock_response
        jobs = JobApi.get_jobs_for_vm_instance_name(mock_config, 'joe')
        self.assertEqual(1, len(jobs))

    @patch('lando.server.jobapi.requests')
    def test_post_error(self, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests.get.return_value = mock_response
        job_api = self.setup_job_api(4)
        job_api.save_error_details('V', 'Out of memory')
        args, kwargs = mock_requests.post.call_args
        self.assertEqual(args[0], 'APIURL/admin/job-errors/')
        self.assertEqual(kwargs['json']['job'], 4)
        self.assertEqual(kwargs['json']['job_step'], 'V')
        self.assertEqual(kwargs['json']['content'], 'Out of memory')

    def test_job_constructor_volume_size(self):
        payload = dict(self.job_response_payload)
        payload['volume_size'] = 1000
        job = Job(payload)
        self.assertEqual(job.volume_size, 1000,
                         "A job payload with volume_size should result in that volume size.")

    @patch('lando.server.jobapi.requests')
    def test_get_store_output_job_data(self, mock_requests):
        """
        Test requesting job status, etc
        """
        job_api = self.setup_job_api(1)

        mock_job_get_response = MagicMock()
        mock_job_get_response.json.return_value = self.job_response_payload

        mock_share_group_response = MagicMock()
        mock_share_group_response.json.return_value = {
            'users': [
                {
                    'dds_id': '123'
                }
            ]
        }

        mock_requests.get.side_effect = [
            mock_job_get_response,
            mock_share_group_response
        ]
        store_output_data = job_api.get_store_output_job_data()

        self.assertEqual(1, store_output_data.id)
        self.assertEqual(23, store_output_data.user_id)
        self.assertEqual('joe@joe.com', store_output_data.username)
        self.assertEqual('N', store_output_data.state)
        self.assertEqual('m1.tiny', store_output_data.vm_flavor)
        self.assertEqual('', store_output_data.vm_instance_name)
        self.assertEqual('jpb67', store_output_data.vm_project_name)

        self.assertEqual('{ "value": 1 }', store_output_data.workflow.job_order)
        self.assertEqual('file:///mnt/fastqc.cwl', store_output_data.workflow.url)
        self.assertEqual('#main', store_output_data.workflow.object_name)

        self.assertEqual(['123'], store_output_data.share_dds_ids)

    @patch('lando.server.jobapi.requests')
    def test_get_run_job_data(self, mock_requests):
        mock_response1 = MagicMock()
        mock_response1.json.return_value = self.job_response_payload
        mock_response2 = MagicMock()
        mock_response2.json.return_value = {'content': '#Markdown data'}
        mock_requests.get.side_effect = [mock_response1, mock_response2]
        job_api = self.setup_job_api(4)
        run_job_data = job_api.get_run_job_data()
        self.assertEqual('myjob', run_job_data.name)
        self.assertEqual('#Markdown data', run_job_data.workflow_methods_document.content)
        mock_requests.get.assert_has_calls([
            call('APIURL/admin/jobs/4/', headers={}),
            call('APIURL/admin/workflow-methods-documents/7', headers={})
        ])
        #args, kwargs = mock_requests.get.call_args
        #self.assertEqual(args[0], 'APIURL/admin/workflow-methods-documents/123')

    @patch('lando.server.jobapi.requests')
    def test_get_workflow_methods_document(self, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = {'content': '#Markdown'}
        mock_requests.get.return_value = mock_response
        job_api = self.setup_job_api(4)
        workflow_methods_document = job_api.get_workflow_methods_document('123')
        self.assertEqual('#Markdown', workflow_methods_document.content)
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/workflow-methods-documents/123')


class TestJob(TestCase):
    def setUp(self):
        self.job_data = {
            'id': 1,
            'user': {
                'id': 23,
                'username': 'joe@joe.com'
            },
            'state': 'N',
            'step': '',
            'name': 'myjob',
            'created': '2017-03-21T13:29:09.123603Z',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            'vm_volume_name': '',
            "vm_project_name": 'jpb67',
            'job_order': '{ "value": 1 }',
            'workflow_version': {
                'name': 'SomeWorkflow',
                'version': 1,
                'url': 'file:///mnt/fastqc.cwl',
                'object_name': '#main',
                "methods_document": 7,
            },
            'output_project': {
                'id': 5,
                'dds_user_credentials': 123
            },
            'stage_group': None,
            'share_group': 42,
            'volume_size': 100,
        }

    def test_cleanup_vm_default(self):
        mock_data = MagicMock()
        job = Job(self.job_data)
        self.assertEqual(job.cleanup_vm, True)

    def test_cleanup_vm_true(self):
        self.job_data['cleanup_vm'] = True
        mock_data = MagicMock()
        job = Job(self.job_data)
        self.assertEqual(job.cleanup_vm, True)

    def test_cleanup_vm_false(self):
        self.job_data['cleanup_vm'] = False
        mock_data = MagicMock()
        job = Job(self.job_data)
        self.assertEqual(job.cleanup_vm, False)
