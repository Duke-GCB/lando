"""
Testing that lando can have top level methods called which will propogate to LandoActions and
perform the expected actions.
"""
from __future__ import absolute_import
from unittest import TestCase
from lando.server.lando import Lando
from mock import MagicMock, patch


LANDO_CONFIG = """
work_queue:
  host: 127.0.0.1
  username: lando
  password: odnal
  worker_username: lobot
  worker_password: tobol
  listen_queue: lando

vm_settings:
  worker_image_name: lando_worker
  ssh_key_name: jpb67
  network_name: selfservice
  floating_ip_pool_name: ext-net
  default_favor_name: m1.small

cloud_settings:
  auth_url: http://10.109.252.9:5000/v3
  username: jpb67
  user_domain_name: Default
  project_name: jpb67
  project_domain_name: Default
  password: secret

job_api:
  url: http://localhost:8000/api
  username: jpb67
  password: secret

fake_cloud_service: True
"""


class Report(object):
    """
    Builds a text document of steps executed by Lando.
    This makes it easier to assert what operations happened.
    """
    def __init__(self):
        self.text = ''

    def add(self, line):
        self.text += line + '\n'

    def make_vm_name(self, job_id):
        self.add("Created vm name for job {}.".format(job_id))
        return "worker_x"

    def launch_instance(self, server_name, flavor_name, script_contents):
        self.add("Launched vm {}.".format(server_name))
        return MagicMock(), MagicMock()

    def terminate_instance(self, server_name):
        self.add("Terminated vm {}.".format(server_name))

    def get_job(self):
        job = MagicMock()
        job.id = '1'
        job.user_id = '1'
        job.state = 'N'
        job.vm_flavor = ''
        job.vm_instance_name = 'worker_x'
        job.vm_project_name = 'bespin_user1'
        job.workflow = MagicMock()
        job.output_directory = MagicMock()
        return job

    def set_job_state(self, state):
        self.add("Set job state to {}.".format(state))

    def set_job_step(self, step):
        self.add("Set job step to {}.".format(step))

    def set_vm_instance_name(self, instance_name):
        self.add("Set vm instance name to {}.".format(instance_name))

    def stage_job(self, credentials, job_id, files, vm_instance_name):
        self.add("Put stage message in queue for {}.".format(vm_instance_name))

    def run_job(self, job_id, workflow, vm_instance_name):
        self.add("Put run_job message in queue for {}.".format(vm_instance_name))

    def store_job_output(self, credentials, job_id, output_directory, vm_instance_name):
        self.add("Put store_job_output message in queue for {}.".format(vm_instance_name))

    def delete_queue(self):
        self.add("Delete my worker's queue.")


def make_mock_settings_and_report(job_id):
    report = Report()
    settings = MagicMock()
    cloud_service = MagicMock()
    cloud_service.make_vm_name = report.make_vm_name
    cloud_service.launch_instance = report.launch_instance
    cloud_service.terminate_instance = report.terminate_instance

    job_api = MagicMock()
    job_api.set_job_state = report.set_job_state
    job_api.set_job_step = report.set_job_step
    job_api.set_vm_instance_name = report.set_vm_instance_name
    job_api.get_job = report.get_job

    worker_client = MagicMock()
    worker_client.stage_job = report.stage_job
    worker_client.run_job = report.run_job
    worker_client.delete_queue = report.delete_queue
    worker_client.store_job_output = report.store_job_output

    settings.get_cloud_service.return_value = cloud_service
    settings.get_job_api.return_value = job_api
    settings.get_worker_client.return_value = worker_client
    settings.job_id = job_id
    return settings, report


class TestLando(TestCase):
    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_start_job(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 1
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.start_job(MagicMock(job_id=job_id))
        expected_report = """
Set job state to R.
Created vm name for job 1.
Set job step to V.
Launched vm worker_x.
Set vm instance name to worker_x.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobApi')
    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_worker_started(self, mock_requests, MockLandoWorkerClient, MockJobSettings, MockJobApi):
        MockJobApi.get_jobs_for_vm_instance_name.return_value = [MagicMock(state="R", step="V")]
        job_id = 1
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.worker_started(MagicMock(worker_queue_name='stuff'))
        expected_report = """
Set job step to S.
Put stage message in queue for stuff.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_cancel_job(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 2
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.cancel_job(MagicMock(job_id=job_id))
        expected_report = """
Set job state to C.
Terminated vm worker_x.
Delete my worker's queue.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_stage_job_complete(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 3
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.stage_job_complete(MagicMock(job_id=1, vm_instance_name='worker_x'))
        expected_report = """
Set job step to R.
Put run_job message in queue for worker_x.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_run_job_complete(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 4
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.run_job_complete(MagicMock(job_id=1, vm_instance_name='worker_x'))
        expected_report = """
Set job step to O.
Put store_job_output message in queue for worker_x.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_stage_job_error(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 4
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.stage_job_error(MagicMock(job_id=1, vm_instance_name='worker_5', message='Oops1'))
        expected_report = """
Set job state to E.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_run_job_error(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 4
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.run_job_error(MagicMock(job_id=1, vm_instance_name='worker_5', message='Oops2'))
        expected_report = """
Set job state to E.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())

    @patch('lando.server.lando.JobSettings')
    @patch('lando.server.lando.LandoWorkerClient')
    @patch('lando.server.jobapi.requests')
    def test_store_job_output_error(self, mock_requests, MockLandoWorkerClient, MockJobSettings):
        job_id = 4
        mock_settings, report = make_mock_settings_and_report(job_id)
        MockJobSettings.return_value = mock_settings
        lando = Lando(MagicMock())
        lando.store_job_output_error(MagicMock(job_id=1, vm_instance_name='worker_5', message='Oops3'))
        expected_report = """
Set job state to E.
        """
        self.assertMultiLineEqual(expected_report.strip(), report.text.strip())
