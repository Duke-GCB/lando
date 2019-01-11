import os
from lando.kube.config import ClusterApiSettings, BespinApiSettings
import logging


class WorkerConfig(object):
    def __init__(self):
        self.job_id = os.environ['JOB_ID']
        self.workflow_dir = os.environ['WORKFLOW_DIR']
        self.bespin_api_settings = BespinApiSettings()
        self.cluster_api = ClusterApiSettings()
        self.log_level = os.environ.get('LOG_LEVEL', logging.WARNING)