"""
Downloads input files and uploads output directory.
"""
import os
import re
import requests
import dateutil.parser
import logging
import ddsc.config
from ddsc.core.remotestore import RemoteStore, RemoteFile
from ddsc.core.download import ProjectDownload
from ddsc.core.filedownloader import FileDownloader
from ddsc.core.util import KindType
from ddsc.core.upload import ProjectUpload
from lando.worker.provenance import create_activity

DOWNLOAD_URL_CHUNK_SIZE = 5 * 1024 # 5KB


def create_parent_directory(path):
    """
    Python equivalent of mkdir -p path.
    :param path: str: path we want to create multiple directories for.
    """
    parent_directory = os.path.dirname(path)
    if not os.path.exists(parent_directory):
        os.mkdir(parent_directory)


class Context(object):
    """
    Holds data to be re-used when uploading multiple files (app and user credentials)
    """
    def __init__(self, credentials):
        """
        :param credentials: jobapi.Credentials
        """
        self.dds_user_credentials = credentials.dds_user_credentials
        self.uploaded_file_ids = []

    def get_duke_data_service(self, user_id):
        """
        Create local DukeDataService after by creating DukeDS config.
        :param user_id: int: bespin user id
        """
        return DukeDataService(self.get_duke_ds_config(user_id))

    def get_duke_ds_config(self, user_id):
        """
        Create DukeDS configuration for a user id
        :param user_id: int: bespin user id
        :return: ddsc.config.Config
        """
        config = ddsc.config.Config()
        credentials = self.dds_user_credentials[user_id]
        config.values[ddsc.config.Config.URL] = credentials.endpoint_api_root
        config.values[ddsc.config.Config.AGENT_KEY] = credentials.endpoint_agent_key
        config.values[ddsc.config.Config.USER_KEY] = credentials.token
        return config


class DukeDataService(object):
    """
    Wraps up ddsc data service that handles upload/download.
    """
    def __init__(self, config):
        """
        :param config: ddsc.config.Config: duke data service configuration
        """
        self.config = config
        self.remote_store = RemoteStore(self.config)
        self.data_service = self.remote_store.data_service

    def download_file(self, file_id, destination_path):
        """
        Download file_id from DukeDS and store it at destination path
        :param file_id: str: duke data service id for ths file
        :param destination_path: str: path to where we will write out the file
        """
        file_data = self.data_service.get_file(file_id).json()
        remote_file = RemoteFile(file_data, '')
        url_json = self.data_service.get_file_url(file_id).json()
        downloader = FileDownloader(self.config, remote_file, url_json, destination_path, self)
        downloader.run()
        ProjectDownload.check_file_size(remote_file, destination_path)

    def transferring_item(self, item, increment_amt=1):
        """
        Called to update progress as a file/folder is transferred.
        :param item: RemoteFile/RemoteFolder: that is being transferrred.
        :param increment_amt: int: allows for progress bar
        """
        logging.info('Transferring {} of {}', increment_amt, item.name)

    def give_user_permissions(self, project_id, username, auth_role):
        logging.info("give user permissions. project:{} username{}: auth_role:{}".format(project_id, username, auth_role))
        remote_user = self.remote_store.lookup_user_by_username(username)
        self.data_service.set_user_project_permission(project_id, remote_user.id, auth_role)

    def create_activity(self, activity_name, desc, started_on, ended_on):
        resp = self.data_service.create_activity(activity_name, desc, started_on, ended_on)
        return resp.json()["id"]

    def create_used_relations(self, activity_id, used_file_ids):
        for file_id in used_file_ids:
            file_version_id = self.get_file_version_id(file_id)
            self.data_service.create_used_relation(activity_id, KindType.file_str, file_version_id)

    def create_generated_by_relations(self, activity_id, generated_file_ids):
        for file_id in generated_file_ids:
            file_version_id = self.get_file_version_id(file_id)
            self.data_service.create_was_generated_by_relation(activity_id, KindType.file_str, file_version_id)

    def get_file_version_id(self, file_id):
        file_info = self.data_service.get_file(file_id).json()
        return file_info['current_version']['id']


class DownloadDukeDSFile(object):
    """
    Downloads a file from DukeDS.
    """
    def __init__(self, file_id, dest, user_id):
        """
        :param file_id: str: unique file id
        :param dest: str: destination we will download the file into
        :param user_id: int: bespin user id
        """
        self.file_id = file_id
        self.dest = dest
        self.user_id = user_id

    def run(self, context):
        """
        Download the file
        :param context: Context
        """
        create_parent_directory(self.dest)
        duke_data_service = context.get_duke_data_service(self.user_id)

        duke_data_service.download_file(self.file_id, self.dest)


class DownloadURLFile(object):
    """
    Downloads a file from a URL.
    """
    def __init__(self, url, destination_path):
        """
        :param url: str: where we will download the file from
        :param destination_path: str: path where we will store the file contents
        """
        self.url = url
        self.destination_path = destination_path

    def run(self, context):
        """
        Download the file
        :param context: Context
        """
        create_parent_directory(self.destination_path)
        r = requests.get(self.url, stream=True)
        with open(self.destination_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_URL_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)


class UploadProject(object):
    """
    Uploads files/folders into a specified project name.
    """
    def __init__(self, project_name, file_folder_list):
        self.project_name = project_name
        self.file_folder_list = file_folder_list

    def run(self, config):
        """
        Upload project and return local project with ids filled in
        :param config: ddsc.config.Config: config settings to use
        :return: ddsc.core.localproject.LocalProject
        """
        project_upload = ProjectUpload(config, self.project_name, self.file_folder_list)
        project_upload.run()
        return project_upload.local_project


class SaveJobOutput(object):
    """
    Saves the output files into a project that is shared with the user.
    """
    def __init__(self, payload):
        """
        :param payload: StoreJobOutputPayload: info about how to store our results
        """
        self.context = Context(payload.credentials)
        self.project_name = SaveJobOutput.create_project_name(payload)
        self.worker_credentials = payload.job_details.output_project.dds_user_credentials
        self.share_with_username = payload.job_details.username
        self.job_details = payload.job_details

    def run(self, working_directory):
        """
        Upload files to DukeDS into a new project.
        :param working_directory: str: directory that contains our output files
        :return: LocalProject: project that was uploaded that now contains remote ids
        """
        config = self.context.get_duke_ds_config(self.worker_credentials)
        upload_paths = [os.path.join(working_directory, path) for path in os.listdir(working_directory)]
        upload_project = UploadProject(self.project_name, upload_paths)
        project = upload_project.run(config)
        self._create_activity(working_directory, project)
        self._share_project(project)
        return project

    def _create_activity(self, working_directory, project):
        """
        Create an activity and relationships for this uploaded project
        :param working_directory: str: directory that contains our output files
        :param project: ddsc.core.localproject.LocalProject: contains ids of uploaded projects
        """
        data_service = self.context.get_duke_data_service(self.worker_credentials)
        create_activity(data_service, self.job_details, working_directory, project)

    def _share_project(self, project):
        """
        Share project with the appropriate user since it has been uploaded.
        :param project: ddsc.core.localproject.LocalProject: contains ids of uploaded projects
        """
        data_service = self.context.get_duke_data_service(self.worker_credentials)
        data_service.give_user_permissions(project.remote_id,
                                           self.get_dukeds_username(),
                                           auth_role='project_admin')

    def get_dukeds_username(self):
        """
        Formats the username provided by bespin-api for use with DukeDS (removes the domain).
        :return: str: DukeDS format username
        """
        return re.sub("@.*", "", self.share_with_username)

    @staticmethod
    def create_project_name(payload):
        """
        Creates a unique project name for the output project.
        :param payload: StoreJobOutputPayload: info about how to store our results
        :return: str: name to use for the project
        """
        job_details = payload.job_details
        job_name = job_details.name
        job_created = dateutil.parser.parse(job_details.created).strftime("%Y-%m-%d")
        workflow = job_details.workflow
        workflow_name = workflow.name
        workflow_version = workflow.version
        return "Bespin {} v{} {} {}".format(workflow_name, workflow_version, job_name, job_created)
