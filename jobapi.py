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

    def get_input_files(self, job_id):
        path = 'job-input-files/?job={}'.format(job_id)
        url = self._make_url(path)
        resp = requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def _make_url(self, suffix):
        return '{}/{}'.format(self.settings.url, suffix)

    def get_dds_user_credentials(self, user_id):
        path = 'dds-user-credentials/?user={}'.format(user_id)
        url = self._make_url(path)
        resp = requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def get_dds_app_credentials(self):
        path = 'dds-app-credentials/'
        url = self._make_url(path)
        resp = requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()


class JobApi(object):
    def __init__(self, config, job_id):
        self.api = BespinApi(config)
        self.job_id = job_id

    def get_job(self):
        return Job(self.api.get_job(self.job_id))

    def set_job_state(self, state):
        self.set_job({'state': state})

    def set_vm_instance_name(self, vm_instance_name):
        self.set_job({'vm_instance_name': vm_instance_name})

    def set_job(self, params):
        self.api.put_job(self.job_id, params)

    def get_input_files(self):
        fields = self.api.get_input_files(self.job_id)
        return [InputFile(field) for field in fields]

    def get_credentials(self):
        job = self.get_job()
        user_id = job.user_id
        credentials = Credentials()

        for user_credential_data in self.api.get_dds_user_credentials(user_id):
            credentials.add_user_credential(DDSUserCredential(user_credential_data))

        for app_credential_data in self.api.get_dds_app_credentials():
            credentials.add_app_credential(DDSAppCredential(app_credential_data))

        return credentials


class Job(object):
    def __init__(self, data):
        self.id = data['id']
        self.user_id = data['user_id']
        self.state = data['state']
        self.vm_flavor = data['vm_flavor']
        self.vm_instance_name = data['vm_instance_name']
        self.workflow = Workflow(data)
        self.output_directory = OutputDirectory(data)


class Workflow(object):
    def __init__(self, data):
        self.input_json = data['workflow_input_json']
        workflow_version = data['workflow_version']
        self.url = workflow_version['url']
        self.object_name = workflow_version['object_name']
        self.output_directory = data['output_dir']['dir_name']


class OutputDirectory(object):
    def __init__(self, data):
        output_dir = data['output_dir']
        self.dir_name = output_dir['dir_name']
        self.project_id = output_dir['project_id']
        self.dds_app_credentials = output_dir['dds_app_credentials']
        self.dds_user_credentials = output_dir['dds_user_credentials']


class InputFile(object):
    def __init__(self, data):
        self.file_type = data['file_type']
        self.workflow_name = data['workflow_name']
        self.dds_files = [DukeDSFile(field) for field in data['dds_files']]
        self.url_files = [URLFile(field) for field in data['url_files']]

    def __str__(self):
        return 'Input file "{}" ({})'.format(self.workflow_name, self.file_type)


class DukeDSFile(object):
    def __init__(self, data):
        self.file_id = data['file_id']
        self.destination_path = data['destination_path']
        self.agent_id = data['dds_app_credentials']
        self.user_id = data['dds_user_credentials']


class URLFile(object):
    def __init__(self, data):
        self.url = data['url']
        self.destination_path = data['destination_path']
        

class Credentials(object):
    def __init__(self):
        self.dds_app_credentials = {}
        self.dds_user_credentials = {}

    def add_app_credential(self, app_credential):
        self.dds_app_credentials[app_credential.id] = app_credential

    def add_user_credential(self, user_credential):
        self.dds_user_credentials[user_credential.id] = user_credential


class DDSAppCredential(object):
    def __init__(self, data):
        self.id = data['id']
        self.name = data['name']
        self.agent_key = data['agent_key']
        self.api_root = data['api_root']

    def __str__(self):
        return "{} : {}".format(self.api_root, self.agent_key)


class DDSUserCredential(object):
    def __init__(self, data):
        self.id = data['id']
        self.user = data['user']
        self.token = data['token']

    def __str__(self):
        return self.token


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


"""
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
        input_files = self.job_api.get_input_files()
        for field in :
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
"""