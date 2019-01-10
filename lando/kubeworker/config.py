import os


class WorkerConfig(object):
    def __init__(self):
        self.job_id = os.environ['JOB_ID']
        self.workflow_dir = os.environ['WORKFLOW_DIR']
        self.bespin_api_settings = BespinEnvConfig()
        self.cluster_api = ClusterEnvConfig()


class BespinEnvConfig(object):
    def __init__(self):
        self.url = os.environ['BESPIN_URL']
        self.token = os.environ['BESPIN_TOKEN']


class ClusterEnvConfig(object):
    def __init__(self):
        self.host = os.environ['CLUSTER_HOST']
        self.token = os.environ['CLUSTER_TOKEN']
        self.namespace = os.environ['CLUSTER_NAMESPACE']
