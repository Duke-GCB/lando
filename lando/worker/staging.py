"""
Downloads input files and uploads output directory.
"""
from __future__ import print_function
import os
import requests
import logging
import ddsc.config
from ddsc.core.remotestore import RemoteStore, RemoteFile
from ddsc.core.download import ProjectDownload
from ddsc.core.filedownloader import FileDownloader
from ddsc.core.util import KindType
from ddsc.core.fileuploader import FileUploader
from ddsc.core.localstore import LocalFile
from ddsc.core.upload import ProjectUpload

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
