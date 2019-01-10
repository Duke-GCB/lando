from lando.server.lando import JobApi, JobStates
from lando.kube.cluster import ClusterApi, BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, SecretEnvVar
import logging
import urllib
import os


class Worker(object):
    def __init__(self, config):
        self.config = config
        self.job_api = JobApi(config=config, job_id=config.job_id)
        self.job = self.job_api.get_job()
        self.workflow = Workflow(config, self.job)
        self.cluster_api = ClusterApi(config.cluster_api.host,
                                      config.cluster_api.token,
                                      config.cluster_api.namespace,
                                      incluster_config=config.cluster_api.incluster_config,
                                      verify_ssl=False)  # TODO REMOVE THIS

    def run(self):
        if self.job.state == JobStates.STARTING:
            self.start_job()
        elif self.job.state == JobStates.RUNNING:
            self.restart_job()
        else:
            logging.error("Invalid job state {}".format())

    def start_job(self):
        self.add_workflow_to_volume()
        self.stage_data()
        self.run_workflow()
        self.store_output()

    def restart_job(self):
        raise NotImplemented("TODO")

    def add_workflow_to_volume(self):
        self.workflow.download_workflow()
        self.workflow.write_job_order_file()

    def stage_data(self):
        pass

    def run_workflow(self):
        pass

    def store_output(self):
        pass


class Workflow(object):
    def __init__(self, config, job):
        self.config = config
        self.job = job

    @property
    def workflow_path(self):
        workflow_filename = os.path.basename(self.job.workflow.url)
        return '{}/{}'.format(self.config.workflow_dir, workflow_filename)

    @property
    def job_order_path(self):
        return '{}/job-order.json'.format(self.config.workflow_dir)

    def download_workflow(self):
        urllib.request.urlretrieve(self.job.workflow.url, self.workflow_path)

    def write_job_order_file(self):
        with open(self.job_order_path, 'w') as outfile:
            outfile.write(self.job.workflow.job_order)
