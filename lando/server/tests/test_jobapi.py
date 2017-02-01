from __future__ import absolute_import
from unittest import TestCase
from lando.server.jobapi import JobApi, BespinApi
from mock.mock import MagicMock, patch


class TestJobApi(TestCase):
    def setup_job_api(self, job_id):
        mock_config = MagicMock()
        mock_config.bespin_api_settings.url = 'APIURL'
        job_api = JobApi(mock_config, job_id)
        return job_api

    @patch('lando.server.jobapi.requests')
    def test_get_job_api(self, mock_requests):
        """
        Test requesting job status, etc
        """
        job_api = self.setup_job_api(1)
        job_response_payload = {
            'id': 1,
            'user_id': 23,
            'state': 'N',
            'step': '',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            "vm_project_name": 'jpb67',
            'job_order': '{ "value": 1 }',
            'workflow_version': {
                'url': 'file:///mnt/fastqc.cwl',
                'object_name': '#main',
            },
            'output_dir': {
                'dir_name': 'results',
                'project_id': '1235123',
                'dds_user_credentials': '123',
            },
        }
        mock_response = MagicMock()
        mock_response.json.return_value = job_response_payload
        mock_requests.get.return_value = mock_response
        job = job_api.get_job()
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/jobs/1/')

        self.assertEqual(1, job.id)
        self.assertEqual(23, job.user_id)
        self.assertEqual('N', job.state)
        self.assertEqual('m1.tiny', job.vm_flavor)
        self.assertEqual('', job.vm_instance_name)
        self.assertEqual('jpb67', job.vm_project_name)

        self.assertEqual('{ "value": 1 }', job.workflow.job_order)
        self.assertEqual('file:///mnt/fastqc.cwl', job.workflow.url)
        self.assertEqual('#main', job.workflow.object_name)
        self.assertEqual('results', job.workflow.output_directory)

        self.assertEqual('results', job.output_directory.dir_name)
        self.assertEqual('1235123', job.output_directory.project_id)
        self.assertEqual('123', job.output_directory.dds_user_credentials)

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
    def test_get_input_files(self, mock_requests):
        input_files_response_payload = [
            {
                'file_type': 'dds_file',
                'workflow_name': 'sequence',
                'dds_files': [
                    {
                        'file_id': 123,
                        'destination_path': 'seq1.fasta',
                        'dds_user_credentials': 823,
                    }
                ],
                'url_files': [],
            },
            {
                'file_type': 'url_file_array',
                'workflow_name': 'models',
                'dds_files': [],
                'url_files': [
                    {
                        'url': "https://stuff.com/file123.model",
                        'destination_path': "file123.model",
                    }
                ],
            },
        ]


        mock_response = MagicMock()
        mock_response.json.return_value = input_files_response_payload
        mock_requests.get.return_value = mock_response
        job_api = self.setup_job_api(4)
        files = job_api.get_input_files()
        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'APIURL/admin/job-input-files/?job=4')

        self.assertEqual(2, len(files))
        file = files[0]
        self.assertEqual('dds_file', file.file_type)
        self.assertEqual(1, len(file.dds_files))
        dds_file = file.dds_files[0]
        self.assertEqual(123, dds_file.file_id)
        self.assertEqual('seq1.fasta', dds_file.destination_path)
        self.assertEqual(823, dds_file.user_id)

        file = files[1]
        self.assertEqual('url_file_array', file.file_type)
        self.assertEqual(0, len(file.dds_files))
        self.assertEqual(1, len(file.url_files))
        url_file = file.url_files[0]
        self.assertEqual('https://stuff.com/file123.model', url_file.url)
        self.assertEqual('file123.model', url_file.destination_path)

    @patch('lando.server.jobapi.requests')
    def test_get_credentials(self, mock_requests):
        job_response_payload = {
            'id': 4,
            'user_id': 23,
            'state': 'N',
            'step': '',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            "vm_project_name": 'jpb67',
            'job_order': '{ "value": 1 }',
            'workflow_version': {
                'url': 'file:///mnt/fastqc.cwl',
                'object_name': '#main',
            },
            'output_dir': {
                'dir_name': 'results',
                'project_id': '1235123',
                'dds_app_credentials': '456',
                'dds_user_credentials': '123',
            },
        }
        user_credentials_response = [
            {
                'id': 5,
                'user': 23,
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
        self.assertEqual(args[0], 'APIURL/admin/dds-user-credentials/?user=23')

        user_cred = user_credentials.dds_user_credentials[5]
        self.assertEqual('1239109', user_cred.token)
        self.assertEqual('2191230', user_cred.endpoint_agent_key)
        self.assertEqual('localhost/api/v1/', user_cred.endpoint_api_root)


    @patch('lando.server.jobapi.requests')
    def test_get_jobs_for_vm_instance_name(self, mock_requests):
        jobs_response = [
            {
                'id': 1,
                'user_id': 23,
                'state': 'N',
                'step': '',
                'vm_flavor': 'm1.tiny',
                'vm_instance_name': '',
                "vm_project_name": 'jpb67',
                'job_order': '{ "value": 1 }',
                'workflow_version': {
                    'url': 'file:///mnt/fastqc.cwl',
                    'object_name': '#main',
                },
                'output_dir': {
                    'dir_name': 'results',
                    'project_id': '1235123',
                    'dds_user_credentials': '123',
                },
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
