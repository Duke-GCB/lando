import os
from lando.kube.config import ClusterApiSettings, BespinApiSettings


class WorkerConfig(object):
    def __init__(self):
        self.job_id = os.environ['JOB_ID']
        self.workflow_dir = os.environ['WORKFLOW_DIR']
        self.bespin_api_settings = BespinApiSettings()
        self.cluster_api = ClusterApiSettings()
