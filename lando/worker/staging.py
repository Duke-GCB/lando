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
        self.duke_ds = None
        self.current_app_cred = None
        self.current_user_cred = None

    def get_duke_data_service(self, user_id):
        """
        Create local DukeDataService after by creating DukeDS config.
        :param user_id: int: bespin user id
        """
        config = ddsc.config.Config()
        credentials = self.dds_user_credentials[user_id]
        config.values[ddsc.config.Config.URL] = credentials.endpoint_api_root
        config.values[ddsc.config.Config.AGENT_KEY] = credentials.endpoint_agent_key
        config.values[ddsc.config.Config.USER_KEY] = credentials.token
        return DukeDataService(config)


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

    def upload_file(self, project_id, parent_id, parent_kind, source_path, destination_path):
        """
        Upload into project_id the file at source_path and store it at destination_path in the project.
        :param project_id:  str: DukeDS project uuid
        :param parent_id: str: DukeDS uuid of the parent of this file: a folder or project(project_id above)
        :param parent_kind: str: str type of parent ('dds-project' or 'dds-folder')
        :param source_path: str: path to our file we will upload
        :param destination_path: str: path to where we will save the file in the DukeDS project
        """
        local_file = LocalFile(source_path)
        local_file.remote_id = self.get_file_id(parent_id, parent_kind, os.path.basename(destination_path))
        local_file.need_to_send = True
        file_content_sender = FileUploader(self.config, self.data_service, local_file, self)
        file_content_sender.upload(project_id, parent_kind, parent_id)

    def find_or_create_directory(self, parent_id, parent_kind, child_name):
        """
        Find project/folder object or directory in DukeDS.
        :param parent_id: str: unique id of the parent
        :param parent_kind: kind of the parent (folder or project)
        :param child_name: str: name of the directory to create
        """
        child_id, child_kind = self.find_child(parent_id, parent_kind, child_name)
        if not child_id:
            child = self.data_service.create_folder(folder_name=child_name, parent_uuid=parent_id,
                                                    parent_kind_str=parent_kind).json()
            return child['id'], child['kind']
        return child_id, child_kind

    def find_child(self, parent_id, parent_kind, child_name):
        """
        Search for a folder/file in with the specified parent and having child_name.
        :param parent_id: unique id of the parent
        :param parent_kind: kind of the parent (folder or project)
        :param child_name: name of the child to find
        """
        children = None
        if parent_kind == KindType.project_str:
            children = self.data_service.get_project_children(parent_id, child_name).json()['results']
        elif parent_kind == KindType.folder_str:
            children = self.data_service.get_folder_children(parent_id, child_name).json()['results']
        if len(children) > 0:
            child = children[0]
            return child['id'], child['kind']
        else:
            return None, None

    def get_file_id(self, parent_id, parent_kind, filename):
        """
        Lookup unique file id for a filename with a given parent.
        :param parent_id: str: unique id of the parent
        :param parent_kind: str: kind of the parent (folder or project)
        :param filename: str: name of the file
        :return: str: unique id of this file
        """
        file_id, file_kind = self.find_child(parent_id, parent_kind, filename)
        return file_id

    def create_folder(self, parent_id, parent_kind, folder_name):
        """
        Create a folder with folder_name under the parent specified by parent_id/parent_kind.
        :param parent_id: str: unique id of the parent
        :param parent_kind: str: kind of the parent (dds-folder or dds-project)
        :param folder_name: str: name of the folder to create
        :return: str,str: uuid and kind of the folder
        """
        response = self.data_service.create_folder(folder_name,parent_kind, parent_id).json()
        return response['id'], response['kind']


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


class UploadDukeDSFile(object):
    """
    Uploads a file to DukeDS.
    """
    def __init__(self, project_id, parent_id, parent_kind, src, dest):
        """
        :param project_id: str: unique project id
        :param parent_id: str: unique id of the parent
        :param parent_kind: str: kind of the parent (dds-folder or dds-project)
        :param src: str: path to the file we want to upload
        :param dest: str: path to where we want to upload the file in the project
        """
        self.project_id = project_id
        self.parent_id = parent_id
        self.parent_kind = parent_kind
        self.src = src
        self.dest = dest

    def run(self, duke_data_service):
        """
        Upload the file
        :param duke_data_service: DukeDataService
        """
        duke_data_service.upload_file(self.project_id, self.parent_id, self.parent_kind, self.src, self.dest)


class UploadDukeDSFolder(object):
    """
    Uploads a folder and the files it contains to DukeDS.
    """
    def __init__(self, project_id, parent_id, parent_kind, src, dest, user_id):
        """

        :param project_id: str: unique id of the project
        :param parent_id: str: unique id of the parent
        :param parent_kind: str: kind of the parent (dds-folder or dds-project)
        :param src: str: path to folder on disk
        :param dest: str: path to where in the project we will upload to
        :param user_id: int: bespin user id
        """
        self.project_id = project_id
        self.parent_id = parent_id
        self.parent_kind = parent_kind
        self.src = src
        self.dest = dest
        self.user_id = user_id

    def run(self, context):
        """
        Upload folder and it's files.
        :param context: Context
        """
        duke_data_service = context.get_duke_data_service(self.user_id)
        folder_id, folder_kind = duke_data_service.create_folder(self.parent_id, self.parent_kind, self.dest)
        for filename in os.listdir(self.src):
            path = os.path.join(self.src, filename)
            if os.path.isdir(path):
                child_folder_name = os.path.basename(path)
                child_folder = UploadDukeDSFolder(self.project_id, folder_id, folder_kind, path,
                                                  child_folder_name, self.user_id)
                child_folder.run(context)
            else:
                upload_file = UploadDukeDSFile(folder_id, folder_kind, path, os.path.join(self.dest, filename))
                upload_file.run(duke_data_service)
