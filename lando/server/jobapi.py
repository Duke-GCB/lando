"""
Allows reading and updating job information when talking to Bespin REST api.
"""
from __future__ import print_function
import requests
from requests.auth import HTTPBasicAuth


class BespinApi(object):
    """
    Low level api that interfaces with the bespin REST api.
    """
    def __init__(self, config):
        """
        :param config: ServerConfig: contains settings for connecting to REST api
        """
        self.settings = config.job_api_settings
        self.requests = requests # Allows mocking requests for unit testing

    def auth(self):
        """
        Create http auth based on config passed in constructor.
        :return: HTTPBasicAuth
        """
        return HTTPBasicAuth(self.settings.username, self.settings.password)

    def get_job(self, job_id):
        """
        Get dictionary of info about a job.
        :param job_id: int: unique job id
        :return: dict: job details
        """
        path = 'jobs/{}/'.format(job_id)
        url = self._make_url(path)
        resp = self.requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def put_job(self, job_id, data):
        """
        Update a job with some fields.
        :param job_id: int: unique job id
        :param data: dict: params we want to update on the job
        :return: dict: put response
        """
        path = 'jobs/{}/'.format(job_id)
        url = self._make_url(path)
        headers = {'Content-type': 'application/json'}
        resp = self.requests.put(url, auth=self.auth(), json=data)
        resp.raise_for_status()
        return resp.json()

    def get_input_files(self, job_id):
        """
        Get the list of input files(files that need to be staged) for a job.
        :param job_id: int: unique job id
        :return: [dict]: list of input file data
        """
        path = 'job-input-files/?job={}'.format(job_id)
        url = self._make_url(path)
        resp = self.requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def _make_url(self, suffix):
        return '{}/{}'.format(self.settings.url, suffix)

    def get_dds_user_credentials(self, user_id):
        """
        Get the duke data service user credentials for a user id.
        :param user_id: int: bespin user id
        :return: dict: credentials details
        """
        path = 'dds-user-credentials/?user={}'.format(user_id)
        url = self._make_url(path)
        resp = self.requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()

    def get_dds_app_credentials(self):
        """
        Get the all duke data service app credentials.
        :return: [dict]: list of app credentials
        """
        path = 'dds-app-credentials/'
        url = self._make_url(path)
        resp = self.requests.get(url, auth=self.auth())
        resp.raise_for_status()
        return resp.json()


class JobApi(object):
    """
    Allows communicating with bespin job api for a particular job.
    """
    def __init__(self, config, job_id):
        """
        :param config: ServerConfig: contains settings for connecting to REST api
        :param job_id: int: unique job id we want to work with
        """
        self.api = BespinApi(config)
        self.job_id = job_id

    def get_job(self):
        """
        Get information about our job.
        :return: Job: contains properties about this job
        """
        return Job(self.api.get_job(self.job_id))

    def set_job_state(self, state):
        """
        Change the state of the job to the passed value.
        :param state: str: value from JobStates
        """
        self._set_job({'state': state})

    def set_vm_instance_name(self, vm_instance_name):
        """
        Set the vm instance name that this job is being run on.
        :param vm_instance_name: str: openstack instance name
        """
        self._set_job({'vm_instance_name': vm_instance_name})

    def _set_job(self, params):
        self.api.put_job(self.job_id, params)

    def get_input_files(self):
        """
        Get the list of input files(files that need to be staged) for a job.
        :return: [InputFile]: list of files to be downloaded.
        """
        fields = self.api.get_input_files(self.job_id)
        return [InputFile(field) for field in fields]

    def get_credentials(self):
        """
        Get all dds user/app credentials attached to this job.
        :return: Credentials: let's user lookup credential info based on bespin user/app ids.
        """
        job = self.get_job()
        user_id = job.user_id
        credentials = Credentials()

        for user_credential_data in self.api.get_dds_user_credentials(user_id):
            credentials.add_user_credential(DDSUserCredential(user_credential_data))

        for app_credential_data in self.api.get_dds_app_credentials():
            credentials.add_app_credential(DDSAppCredential(app_credential_data))

        return credentials


class Job(object):
    """
    Top level job information.
    """
    def __init__(self, data):
        """
        :param data: dict: job values returned from bespin.
        """
        self.id = data['id']
        self.user_id = data['user_id']
        self.state = data['state']
        self.vm_flavor = data['vm_flavor']
        self.vm_instance_name = data['vm_instance_name']
        self.workflow = Workflow(data)
        self.output_directory = OutputDirectory(data)


class Workflow(object):
    """
    The workflow we should run as part of a job. Returned from bespin.
    """
    def __init__(self, data):
        """
        :param data: dict: workflow values returned from bespin.
        """
        self.input_json = data['workflow_input_json']
        workflow_version = data['workflow_version']
        self.url = workflow_version['url']
        self.object_name = workflow_version['object_name']
        self.output_directory = data['output_dir']['dir_name']


class OutputDirectory(object):
    """
    Information about the directory we should send the result of the workflow to.
    """
    def __init__(self, data):
        """
        :param data: dict: output directory values returned from bespin.
        """
        output_dir = data['output_dir']
        self.dir_name = output_dir['dir_name']
        self.project_id = output_dir['project_id']
        self.dds_app_credentials = output_dir['dds_app_credentials']
        self.dds_user_credentials = output_dir['dds_user_credentials']


class InputFile(object):
    """
    Represents dds/url file or array of files.
    """
    def __init__(self, data):
        """
        :param data: dict: input file values returned from bespin.
        """
        self.file_type = data['file_type']
        self.workflow_name = data['workflow_name']
        self.dds_files = [DukeDSFile(field) for field in data['dds_files']]
        self.url_files = [URLFile(field) for field in data['url_files']]

    def __str__(self):
        return 'Input file "{}" ({})'.format(self.workflow_name, self.file_type)


class DukeDSFile(object):
    """
    Information about a duke ds file that we will download during job staging.
    """
    def __init__(self, data):
        """
        :param data: dict: duke data service file values returned from bespin.
        """
        self.file_id = data['file_id']
        self.destination_path = data['destination_path']
        self.agent_id = data['dds_app_credentials']
        self.user_id = data['dds_user_credentials']


class URLFile(object):
    """
    Information about a url we will download during job staging.
    """
    def __init__(self, data):
        """
        :param data: dict: url values returned from bespin.
        """
        self.url = data['url']
        self.destination_path = data['destination_path']
        

class Credentials(object):
    """
    Keys for downloading from remote storage.
    """
    def __init__(self):
        self.dds_app_credentials = {}
        self.dds_user_credentials = {}

    def add_app_credential(self, app_credential):
        """
        Add app credential to app dictionary for app_credential.id
        :param app_credential: DDSAppCredential
        :return:
        """
        self.dds_app_credentials[app_credential.id] = app_credential

    def add_user_credential(self, user_credential):
        """
        Add app credential to user dictionary for user_credential.id
        :param user_credential: DDSUserCredential
        """
        self.dds_user_credentials[user_credential.id] = user_credential


class DDSAppCredential(object):
    """
    Contains url and agent key for talking to DukeDS.
    """
    def __init__(self, data):
        """
        :param data: dict: app credential values returned from bespin.
        """
        self.id = data['id']
        self.name = data['name']
        self.agent_key = data['agent_key']
        self.api_root = data['api_root']

    def __str__(self):
        return "{} : {}".format(self.api_root, self.agent_key)


class DDSUserCredential(object):
    """
    Contains user key for talking to DukeDS.
    """
    def __init__(self, data):
        """
        :param data: dict: user credential values returned from bespin.
        """
        self.id = data['id']
        self.user = data['user']
        self.token = data['token']

    def __str__(self):
        return self.token


class JobStates(object):
    """
    Values for state that must match up those supported by Bespin.
    """
    NEW = 'N'
    CREATE_VM = 'V'
    STAGING = 'S'
    RUNNING = 'R'
    STORING_JOB_OUTPUT = 'O'
    TERMINATE_VM = 'T'
    FINISHED = 'F'
    ERRORED = 'E'
    CANCELED = 'C'