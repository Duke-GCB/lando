import yaml
import logging
import os
from lando.exceptions import get_or_raise_config_exception, InvalidConfigException
from lando.server.config import WorkQueue, BespinApiSettings


def create_server_config(filename):
    with open(filename, 'r') as infile:
        data = yaml.load(infile)
        if not data:
            raise InvalidConfigException("Empty config file {}.".format(filename))
        return ServerConfig(data)


class ServerConfig(object):
    """
    Configuration for either Server or Client.
    For worker, cloud_settings will return None.
    """
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
        self.data_store_settings = DataStoreSettings(
            get_or_raise_config_exception(data, 'data_store_settings')
        )
        self.stage_data_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'stage_data_settings')
        )
        self.run_workflow_settings = RunWorkflowSettings(
            get_or_raise_config_exception(data, 'run_workflow_settings')
        )
        self.organize_output_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'organize_output_settings')
        )
        self.save_output_settings = ImageCommandSettings(
            get_or_raise_config_exception(data, 'save_output_settings')
        )
        self.storage_class_name = data.get('storage_class_name', 'glusterfs-storage')


class ClusterApiSettings(object):
    """
    Settings used to talk to be Bespin job api.
    """
    def __init__(self, data):
        self.host = get_or_raise_config_exception(data, 'host')
        self.token = get_or_raise_config_exception(data, 'token')
        self.namespace = get_or_raise_config_exception(data, 'namespace')


class ImageCommandSettings(object):
    def __init__(self, data):
        self.image_name = get_or_raise_config_exception(data, 'image_name')
        self.command = get_or_raise_config_exception(data, 'command')
        self.requested_cpu = get_or_raise_config_exception(data, 'requested_cpu')
        self.requested_memory = get_or_raise_config_exception(data, 'requested_memory')


class RunWorkflowSettings(object):
    def __init__(self, data):
        self.requested_cpu = get_or_raise_config_exception(data, 'requested_cpu')
        self.requested_memory = get_or_raise_config_exception(data, 'requested_memory')
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
