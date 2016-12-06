"""
Testing that lando can have top level methods called which will propogate to LandoActions and
perform the expected actions.
"""

from __future__ import absolute_import
from unittest import TestCase
import tempfile
from lando.server.lando import Lando
from lando.server.config import ServerConfig
from lando.messaging.messaging import StartJobPayload, CancelJobPayload
from lando.messaging.messaging import JobStepCompletePayload, JobStepErrorPayload


def write_temp_return_filename(data):
    """
    Write out data to a temporary file and return that file's name.
    :param data: str: data to be written to a file
    :return: str: temp filename we just created
    """
    file = tempfile.NamedTemporaryFile(delete=False)
    file.write(data)
    file.close()
    return file.name


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

class TestLando(TestCase):
    def make_job_settings(self, job_id, config):
        self.fake_job_settings = FakeJobSettings(job_id, config)
        return self.fake_job_settings

    def _make_lando(self):
        config_filename = write_temp_return_filename(LANDO_CONFIG)
        config = ServerConfig(config_filename)
        lando = Lando(config)
        lando._make_job_settings = self.make_job_settings
        return lando

    def test_start_job(self):
        lando = self._make_lando()
        lando.start_job(StartJobPayload(1))
        report = """
Created vm name for job 1.
Set job state to V.
Launched vm worker_1.
Set job state to S.
Making worker client for queue worker_1.
Stage job 1 on worker_1.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_cancel_job(self):
        lando = self._make_lando()
        lando.cancel_job(CancelJobPayload(2))
        report = """
Set job state to C.
Terminated vm worker_x.
Making worker client for queue worker_x.
Delete my worker's queue.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_stage_job_complete(self):
        lando = self._make_lando()

        lando.stage_job_complete(JobStepCompletePayload(FakeJobRequestPayload(1, 'worker_3')))
        report = """
Set job state to R.
Making worker client for queue worker_3.
Run job 1 on worker_3.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_run_job_complete(self):
        lando = self._make_lando()
        lando.run_job_complete(JobStepCompletePayload(FakeJobRequestPayload(1, 'worker_4')))
        report = """
Set job state to O.
Making worker client for queue worker_4.
Store output for job 1 on worker_4.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_stage_job_error(self):
        lando = self._make_lando()
        lando.stage_job_error(JobStepErrorPayload(FakeJobRequestPayload(1, 'worker_5'), message='Oops1'))
        report = """
Set job state to E.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_run_job_error(self):
        lando = self._make_lando()
        lando.run_job_error(JobStepErrorPayload(FakeJobRequestPayload(1, 'worker_5'), message='Oops2'))
        report = """
Set job state to E.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

    def test_store_job_output_error(self):
        lando = self._make_lando()
        lando.store_job_output_error(JobStepErrorPayload(FakeJobRequestPayload(1, 'worker_5'), message='Oops3'))
        report = """
Set job state to E.
        """
        self.assertMultiLineEqual(report.strip(), self.fake_job_settings.report.text.strip())

class Report(object):
    """
    Builds a text document of steps executed by Lando.
    This makes it easier to assert what operations happened.
    """
    def __init__(self):
        self.text = ''

    def add(self, line):
        self.text += line + '\n'


class FakeJobSettings(object):
    def __init__(self, job_id, config):
        self.job_id = job_id
        self.config = config
        self.report = Report()
        self.cloud_service = FakeCloudService(self.report)
        self.job_api = FakeJobApi(self.report)
        self.worker_client = FakeLandoWorkerClient(self.report)

    def get_cloud_service(self):
        return self.cloud_service

    def get_job_api(self):
        return self.job_api

    def get_worker_client(self, queue_name):
        self.report.add("Making worker client for queue {}.".format(queue_name))
        return self.worker_client


class FakeCloudService(object):
    def __init__(self, report):
        self.vm_name_job_id = None
        self.report = report

    def make_vm_name(self, job_id):
        self.vm_name_job_id = job_id
        self.report.add("Created vm name for job {}.".format(job_id))
        return 'worker_1'

    def launch_instance(self, vm_instance_name, boot_script_content):
        self.report.add("Launched vm {}.".format(vm_instance_name))
        return None, "127.0.0.1"

    def terminate_instance(self, instance_name):
        self.report.add("Terminated vm {}.".format(instance_name))


class FakeJobApi(object):
    def __init__(self, report):
        self.job_state = None
        self.report = report

    def set_job_state(self, state):
        self.report.add("Set job state to {}.".format(state))
        self.job_state = state

    def get_credentials(self):
        return FakeCredentials()

    def get_input_files(self):
        return []

    def get_job(self):
        return FakeJob()


class FakeJob(object):
    def __init__(self):
        self.vm_instance_name = 'worker_x'
        self.workflow = {}
        self.output_directory = {}


class FakeCredentials(object):
    pass


class FakeLandoWorkerClient(object):
    def __init__(self, report):
        self.report = report

    def stage_job(self, credentials, job_id, input_files, vm_instance_name):
        self.report.add("Stage job {} on {}.".format(job_id, vm_instance_name))
        pass

    def delete_queue(self):
        self.report.add("Delete my worker's queue.")

    def run_job(self, job_id, workflow, vm_instance_name):
        self.report.add("Run job {} on {}.".format(job_id, vm_instance_name))

    def store_job_output(self, credentials, job_id, output_directory, vm_instance_name):
        self.report.add("Store output for job {} on {}.".format(job_id, vm_instance_name))

class FakeJobRequestPayload(object):
    def __init__(self, job_id, vm_instance_name):
        self.job_id = job_id
        self.vm_instance_name = vm_instance_name