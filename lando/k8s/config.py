import yaml
import logging
import os
from lando.exceptions import get_or_raise_config_exception, InvalidConfigException
from lando.server.config import WorkQueue, BespinApiSettings

DEFAULT_REQUESTED_CPU = 1
DEFAULT_REQUESTED_MEMORY = '1G'


def create_server_config(filename):
    with open(filename, 'r') as infile:
        data = yaml.load(infile)
        if not data:
            raise InvalidConfigException("Empty config file {}.".format(filename))
        return ServerConfig(data)


class ServerConfig(object):
    def __init__(self, data):
        self.log_level = data.get('log_level', logging.WARNING)
        self.work_queue_config = WorkQueue(
            get_or_raise_config_exception(data, 'work_queue')
        )
        self.cluster_api_settings = ClusterApiSettings(
            get_or_raise_config_exception(data, 'cluster_api_settings')
        )
        self.bespin_api_settings = BespinApiSettings(
            get_or_raise_config_exception(data, 'bespin_api')
        )
        # data store settings used by staging data and save output
        self.data_store_settings = DataStoreSettings(
            get_or_raise_config_exception(data, 'data_store_settings')
        )
        # settings for staging data in
        self.stage_data_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'stage_data_settings')
        )
        # settings for running a workflow
        self.run_workflow_settings = RunWorkflowSettings(
            get_or_raise_config_exception(data, 'run_workflow_settings')
        )
        # settings for organizing the results of running the workflow
        self.organize_output_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'organize_output_settings')
        )
        # settings for uploading organized results
        self.save_output_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'save_output_settings')
        )
        # settings for recording output project details
        self.record_output_project_settings = RecordOutputProjectSettings(
            get_or_raise_config_exception(data, 'record_output_project_settings')
        )
        self.storage_class_name = data.get('storage_class_name', None)


class ClusterApiSettings(object):
    def __init__(self, data):
        self.host = get_or_raise_config_exception(data, 'host')
        self.token = get_or_raise_config_exception(data, 'token')
        self.namespace = get_or_raise_config_exception(data, 'namespace')
        self.verify_ssl = data.get('verify_ssl', True)


class RecordOutputProjectSettings(object):
    def __init__(self, data):
        self.image_name = get_or_raise_config_exception(data, 'image_name')
        self.service_account_name = get_or_raise_config_exception(data, 'service_account_name')


class ImageCommandSettings(object):
    def __init__(self, data):
        self.image_name = get_or_raise_config_exception(data, 'image_name')
        self.command = get_or_raise_config_exception(data, 'command')
        self.requested_cpu = data.get('requested_cpu', DEFAULT_REQUESTED_CPU)
        self.requested_memory = data.get('requested_memory', DEFAULT_REQUESTED_MEMORY)


class RunWorkflowSettings(object):
    def __init__(self, data):
        self.requested_cpu = data.get('requested_cpu', DEFAULT_REQUESTED_CPU)
        self.requested_memory = data.get('requested_memory', DEFAULT_REQUESTED_MEMORY)
        self.system_data_volume = None
        if 'system_data_volume' in data:
            self.system_data_volume = SystemDataVolume(
                get_or_raise_config_exception(data, 'system_data_volume')
            )


class SystemDataVolume(object):
    def __init__(self, data):
        self.volume_claim_name = get_or_raise_config_exception(data, 'volume_claim_name')
        self.mount_path = get_or_raise_config_exception(data, 'mount_path')


class DataStoreSettings(object):
    def __init__(self, data):
        self.secret_name = get_or_raise_config_exception(data, 'secret_name')
