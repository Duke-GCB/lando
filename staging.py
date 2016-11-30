from __future__ import print_function
import os
import argparse
import yaml
import requests
import logging
from ddsc.config import create_config
from ddsc.core.remotestore import RemoteStore, RemoteFile
from ddsc.core.download import ProjectDownload
from ddsc.core.filedownloader import FileDownloader
from ddsc.core.util import KindType
from ddsc.core.fileuploader import FileUploader
from ddsc.core.localstore import LocalFile

DOWNLOAD_URL_CHUNK_SIZE = 5 * 1024 # 5KB

class StagingData(object):
    def __init__(self, infile):
        data = yaml.load(infile)
        self.downloads = []
        for download_item in data['download']:
            self.downloads.append(self.make_download(download_item))
        self.uploads = []
        for upload_item in data['upload']:
            self.uploads.append(self.make_upload(upload_item))

    @staticmethod
    def make_download(item):
        type = item['type']
        if type == 'DukeDS':
            return DownloadDukeDSFile(item)
        if type == 'url':
            return DownloadURLFile(item)
        raise ValueError("Invalid download item {}".format(item))

    @staticmethod
    def make_upload(item):
        type = item['type']
        if type == 'DukeDS':
            return UploadDukeDSFile(item)
        raise ValueError("Invalid upload item {}".format(item))


def create_parent_directory(path):
    parent_directory = os.path.dirname(path)
    if not os.path.exists(parent_directory):
        os.mkdir(parent_directory)


class Context(object):
    def __init__(self):
        self.duke_ds = None

    def get_duke_data_service(self):
        if not self.duke_ds:
            self.duke_ds = DukeDataService()
        return self.duke_ds


class DukeDataService(object):
    def __init__(self):
        self.config = create_config()
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


class DownloadDukeDSFile(object):
    def __init__(self, data):
        self.file_id = data['file_id']
        self.dest = data['dest']

    def run(self, context):
        create_parent_directory(self.dest)
        duke_data_service = context.get_duke_data_service()
        duke_data_service.download_file(self.file_id, self.dest)
        print("Downloaded {}".format(self.dest))


class DownloadURLFile(object):
    def __init__(self, data):
        self.url = data['url']
        self.dest = data['dest']

    def run(self, context):
        create_parent_directory(self.dest)
        r = requests.get(self.url, stream=True)
        with open(self.dest, 'wb') as f:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_URL_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        print("Downloaded {}".format(self.dest))


class UploadDukeDSFile(object):
    def __init__(self, data):
        self.project_id = data['project_id']
        self.src = data['src']
        self.dest = data['dest']

    def run(self, context):
        config = create_config()
        duke_data_service = context.get_duke_data_service()
        duke_data_service.upload_file(self.project_id, self.src, self.dest)
        print("Uploaded {}".format(self.dest))


class ArgParser(object):
    def __init__(self, download_func, upload_func):
        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers()
        self._add_cmd(name="download",
                      description="Download data files before running a workflow",
                      func=download_func)
        self._add_cmd(name="upload",
                      description="Save output data files after running a workflow",
                      func=upload_func)

    def _add_cmd(self, name, description, func):
        subparser = self.subparsers.add_parser(name, description=description)
        subparser.add_argument("file", type=argparse.FileType('r'))
        subparser.set_defaults(func=func)

    def parse_and_run(self):
        args = self.parser.parse_args()
        if hasattr(args, 'func'):
            args.func(StagingData(args.file))
        else:
            self.parser.print_help()


def download(staging_data):
    context = Context()
    for item in staging_data.downloads:
        item.run(context)

def upload(staging_data):
    context = Context()
    for item in staging_data.uploads:
        item.run(context)


if __name__ == "__main__":
    arg_parser = ArgParser(download_func=download, upload_func=upload)
    arg_parser.parse_and_run()
