from __future__ import print_function
import requests
from config import Config
from requests.auth import HTTPBasicAuth


class BespinApi(object):
    def __init__(self, config):
        self.settings = config.job_api_settings()

    def auth(self):
        return HTTPBasicAuth(self.settings.username, self.settings.password)

    def get_job(self, job_id):
        path = 'jobs/{}/'.format(job_id)
        url = self._make_url(path)
        resp = requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def put_job(self, job_id, data):
        path = 'jobs/{}/'.format(job_id)
        url = self._make_url(path)
        headers = {'Content-type': 'application/json'}
        resp = requests.put(url, auth=self.auth(), json=data)
        resp.raise_for_status()
        return resp.json()

    def get_job_fields(self, job_id, staging_filter):
        path = 'job-params/?job={}'.format(job_id)
        if staging_filter:
            path += '&staging={}'.format(staging_filter)
        url = self._make_url(path)
        resp = requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def _make_url(self, suffix):
        return '{}/{}'.format(self.settings.url, suffix)


class JobApi(object):
    def __init__(self, config, job_id):
        self.api = BespinApi(config)
        self.job_id = job_id
        self.job_info = None
        self.refresh_info()

    def refresh_info(self):
        self.job_info = self.api.get_job(self.job_id)

    def get_vm_flavor(self):
        return self.job_info['vm_flavor']

    def get_job_state(self):
        return self.job_info['state']

    def set_job_state(self, state):
        self.set_job({'state': state})

    def set_vm_instance_name(self, vm_instance_name):
        self.set_job({'vm_instance_name': vm_instance_name})

    def set_job(self, params):
        self.api.put_job(self.job_id, params)
        self.refresh_info()

    def get_job_fields(self, staging=None):
        fields = self.api.get_job_fields(self.job_id, staging)
        return [JobField(field) for field in fields]


class JobField(object):
    def __init__(self, dict):
        self.id = dict['id']
        self.job = dict['job']
        self.name = dict['name']
        self.type = dict['type']
        self.value = dict['value']
        self.staging = dict['staging']
        self.dds_files = dict['dds_files']
        #'id', 'job', 'name', 'type', 'value', 'staging', 'dds_file'

    def __str__(self):
        return 'Field name:{} job:{} name:{} type:{} value:{} staging:{}'.format(
            self.id, self.job, self.name, self.type, self.value, self.staging)

class JobStates(object):
    NEW = 'N'
    CREATE_VM = 'V'
    STAGING = 'S'
    RUNNING = 'R'
    STORING_JOB_OUTPUT = 'O'
    TERMINATE_VM = 'T'
    FINISHED = 'F'
    ERRORED = 'E'
    CANCELED = 'C'


class StagingTypes(object):
    INPUT = 'I'
    OUTPUT = 'O'
    PARAM = 'P'


class LandoJobRunner(object):
    def __init__(self, config, job_id):
        self.config = config
        self.job_id = job_id
        self.job_api = JobApi(config=self.config, job_id=job_id)

    def start_job(self):
        self._print_state()
        server_name = self._start_vm()
        self._print_state()
        self._start_staging(server_name)
        self._print_state()
        return server_name

    def _print_state(self):
        print("Job state {}".format(self.job_api.get_job_state()))

    def _start_vm(self):
        flavor = self.job_api.get_vm_flavor()
        name = "lobot_X2"
        self.job_api.set_job_state(JobStates.CREATE_VM)
        # slow create VM
        self.job_api.set_vm_instance_name(name)
        print("Started vm {} with {}".format(name, flavor))
        return name

    def _start_staging(self, server_name):
        print("Queue staging job for worker name {}".format(server_name))
        print("Staging fields:")
        for field in self.job_api.get_job_fields(StagingTypes.INPUT):
            print(field)
        self.job_api.set_job_state(JobStates.STAGING)

    def run_job(self, server_name):
        print("All fields:")
        for field in self.job_api.get_job_fields():
            print(field)
        self.job_api.set_job_state(JobStates.RUNNING)
        print("Queue run job for worker name {}".format(server_name))
        self._print_state()

    def store_job_output(self, server_name):
        print("Store job output fields:")
        for field in self.job_api.get_job_fields(StagingTypes.OUTPUT):
            print(field)
        print("Queue archive job for worker name {}".format(server_name))
        self.job_api.set_job_state(JobStates.STORING_JOB_OUTPUT)
        self._print_state()

    def terminate_job(self, server_name):
        self.job_api.set_job_state(JobStates.TERMINATE_VM)
        self._print_state()
        print("Terminate {}".format(server_name))
        self.job_api.set_job_state(JobStates.FINISHED)
        self._print_state()


def main():
    config = Config('landoconfig.yml')
    job_id = 1
    job_runner = LandoJobRunner(config, job_id=job_id)
    job_runner.job_api.set_job_state(JobStates.NEW)
    print("Lando receives start job")
    server_name = job_runner.start_job()
    print("")
    print("Lando receives staging complete")
    job_runner = LandoJobRunner(config, job_id=job_id)
    job_runner.run_job(server_name)
    print("")
    print("Lando receives job running complete")
    job_runner = LandoJobRunner(config, job_id=job_id)
    job_runner.store_job_output(server_name)
    print("")
    print("Lando receives archiving complete")
    job_runner = LandoJobRunner(config, job_id=job_id)
    job_runner.terminate_job(server_name)


if __name__ == '__main__':
    main()
