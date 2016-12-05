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
    parent_directory = os.path.dirname(path)
    if not os.path.exists(parent_directory):
        os.mkdir(parent_directory)


class Context(object):
    def __init__(self, credentials):
        self.dds_app_credentials = credentials.dds_app_credentials
        self.dds_user_credentials = credentials.dds_user_credentials
        self.duke_ds = None
        self.current_app_cred = None
        self.current_user_cred = None

    def get_duke_data_service(self, agent_id, user_id):
        config = ddsc.config.Config()
        config.values[ddsc.config.Config.AGENT_KEY] = self.dds_app_credentials[agent_id].agent_key
        config.values[ddsc.config.Config.USER_KEY] = self.dds_user_credentials[user_id].token
        return DukeDataService(config)


class DukeDataService(object):
    def __init__(self, config):
        self.config = config
        self.remote_store = RemoteStore(self.config)
        self.data_service = self.remote_store.data_service

    def download_file(self, file_id, destination_path):
        file_data = self.data_service.get_file(file_id).json()
        remote_file = RemoteFile(file_data, '')
        url_json = self.data_service.get_file_url(file_id).json()
        downloader = FileDownloader(self.config, remote_file, url_json, destination_path, self)
        downloader.run()
        ProjectDownload.check_file_size(remote_file, destination_path)

    def transferring_item(self, item, increment_amt=1):
        logging.info('Transferring {} of {}', increment_amt, item.name)

    def upload_file(self, project_id, source_path, destination_path):
        parent_id, parent_kind = self.find_or_create_parent(project_id, KindType.project_str, destination_path)
        local_file = LocalFile(source_path)
        local_file.remote_id = self.get_file_id(self.data_service, parent_id, parent_kind,
                                                os.path.basename(destination_path))
        local_file.need_to_send = True
        file_content_sender = FileUploader(self.config, self.data_service, local_file, self)
        file_content_sender.upload(project_id, parent_kind, parent_id)

    def find_or_create_parent(self, parent_id, parent_kind, path):
        dirname = os.path.dirname(path)
        if dirname:
            for part in dirname.split(os.path.sep):
                parent_id, parent_kind = self.find_or_create_directory(parent_id, parent_kind, part)
        return parent_id, parent_kind

    def find_or_create_directory(self, parent_id, parent_kind, child_name):
        child_id, child_kind = self.find_child(parent_id, parent_kind, child_name)
        if not child_id:
            child = self.data_service.create_folder(folder_name=child_name, parent_uuid=parent_id,
                                                    parent_kind_str=parent_kind).json()
            return child['id'], child['kind']
        return child_id, child_kind

    def find_child(self, parent_id, parent_kind, child_name):
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

    def get_file_id(self, data_service, parent_id, parent_kind, filename):
        file_id, file_kind = self.find_child(parent_id, parent_kind, filename)
        return file_id

    def create_top_level_folder(self, project_id, folder_name):
        self.data_service.create_folder(folder_name, KindType.project_str, project_id)


class DownloadDukeDSFile(object):
    def __init__(self, file_id, dest, agent_id, user_id):
        self.file_id = file_id
        self.dest = dest
        self.agent_id = agent_id
        self.user_id = user_id

    def run(self, context):
        create_parent_directory(self.dest)
        duke_data_service = context.get_duke_data_service(self.agent_id, self.user_id)

        duke_data_service.download_file(self.file_id, self.dest)


class DownloadURLFile(object):
    def __init__(self, url, destination_path):
        self.url = url
        self.destination_path = destination_path

    def run(self, context):
        create_parent_directory(self.destination_path)
        r = requests.get(self.url, stream=True)
        with open(self.destination_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_URL_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)


class UploadDukeDSFile(object):
    def __init__(self, project_id, src, dest):
        self.project_id = project_id
        self.src = src
        self.dest = dest

    def run(self, duke_data_service):
        duke_data_service.upload_file(self.project_id, self.src, self.dest)


class UploadDukeDSFolder(object):
    def __init__(self, project_id, src, dest, agent_id, user_id):
        self.project_id = project_id
        self.src = src
        self.dest = dest
        self.agent_id = agent_id
        self.user_id = user_id

    def run(self, context):
        duke_data_service = context.get_duke_data_service(self.agent_id, self.user_id)
        duke_data_service.create_top_level_folder(self.project_id, self.dest)
        for filename in os.listdir(self.src):
            path = os.path.join(self.src, filename)
            upload_file = UploadDukeDSFile(self.project_id, path, os.path.join(self.dest, filename))
            upload_file.run(duke_data_service)