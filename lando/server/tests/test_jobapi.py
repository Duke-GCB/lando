from __future__ import absolute_import
from unittest import TestCase
from lando.server.jobapi import JobApi


class FakeConfig(object):
    def __init__(self):
        self.url = '127.0.0.1'
        self.username = 'joe'
        self.password = 'secret'
        self.job_api_settings = self


class FakeRequests(object):
    def __init__(self):
        self.url_to_response = {}
        self.last_url = None
        self.last_json = None

    def _response(self, url, auth, json=None):
        self.last_url = url
        self.last_json = json
        response = self.url_to_response.get(url)
        return response

    def get(self, url, auth, json=None):
        return self._response(url, auth, json)

    def put(self, url, auth, json=None):
        return self._response(url, auth, json)


class FakeResponse(object):
    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data

    def raise_for_status(self):
        pass


class TestJobApi(TestCase):
    def setup_job_api_and_requests(self, job_id):
        fake_requests = FakeRequests()
        job_api = JobApi(FakeConfig(), job_id)
        job_api.api.requests = fake_requests
        return job_api, fake_requests

    def test_get_job_api(self):
        """
        Test requesting job status, etc
        """
        job_api, fake_requests = self.setup_job_api_and_requests(1)
        job_response_payload = {
            'id': 1,
            'user_id': 23,
            'state': 'N',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            'workflow_input_json': '{ "value": 1 }',
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
        fake_requests.url_to_response['127.0.0.1/jobs/1/'] = FakeResponse(job_response_payload)
        job = job_api.get_job()
        self.assertEqual(1, job.id)
        self.assertEqual(23, job.user_id)
        self.assertEqual('N', job.state)
        self.assertEqual('m1.tiny', job.vm_flavor)
        self.assertEqual('', job.vm_instance_name)

        self.assertEqual('{ "value": 1 }', job.workflow.input_json)
        self.assertEqual('file:///mnt/fastqc.cwl', job.workflow.url)
        self.assertEqual('#main', job.workflow.object_name)
        self.assertEqual('results', job.workflow.output_directory)

        self.assertEqual('results', job.output_directory.dir_name)
        self.assertEqual('1235123', job.output_directory.project_id)
        self.assertEqual('123', job.output_directory.dds_user_credentials)

    def test_set_job_state(self):
        job_api, fake_requests = self.setup_job_api_and_requests(2)
        fake_requests.url_to_response['127.0.0.1/jobs/2/'] = FakeResponse({})
        job_api.set_job_state('E')
        self.assertEqual({'state':'E'}, fake_requests.last_json)

    def test_set_vm_instance_name(self):
        job_api, fake_requests = self.setup_job_api_and_requests(3)
        fake_requests.url_to_response['127.0.0.1/jobs/3/'] = FakeResponse({})
        job_api.set_vm_instance_name('worker_123')
        self.assertEqual({'vm_instance_name': 'worker_123'}, fake_requests.last_json)

    def test_get_input_files(self):
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

        job_api, fake_requests = self.setup_job_api_and_requests(4)
        fake_requests.url_to_response['127.0.0.1/job-input-files/?job=4'] = FakeResponse(input_files_response_payload)
        files = job_api.get_input_files()
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

    def test_get_credentials(self):
        job_api, fake_requests = self.setup_job_api_and_requests(4)
        job_response_payload = {
            'id': 4,
            'user_id': 23,
            'state': 'N',
            'vm_flavor': 'm1.tiny',
            'vm_instance_name': '',
            'workflow_input_json': '{ "value": 1 }',
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
                'token': '1239109'
            }
        ]
        app_credentials_response = [
            {
                'id': 3,
                'name': 'dukeds',
                'agent_key': '2191230',
                'api_root': 'localhost/api/v1/',
            }
        ]
        fake_requests.url_to_response['127.0.0.1/jobs/4/'] = FakeResponse(job_response_payload)
        fake_requests.url_to_response['127.0.0.1/dds-user-credentials/?user=23'] = FakeResponse(user_credentials_response)
        fake_requests.url_to_response['127.0.0.1/dds-app-credentials/'] = FakeResponse(app_credentials_response)

        user_credentials = job_api.get_credentials()
        user_cred = user_credentials.dds_user_credentials[5]
        self.assertEqual('1239109', user_cred.token)
        self.assertEqual('2191230', user_cred.endpoint_agent_key)
        self.assertEqual('localhost/api/v1/', user_cred.endpoint_api_root)
