from lando.server.lando import JobApi, JobStates
from lando.kube.cluster import ClusterApi, BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, SecretEnvVar
import logging
import urllib
import os
import json


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
        self.working_directory = '/data'
        self.stage_job_name = "stage-job-{}".format(config.job_id)
        self.ddsclient_agent_name = "ddsclient-agent"
        self.job_claim_name = "job{}-volume".format(config.job_id)

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
        dds_files = []
        input_files = self.job_api.get_input_files()
        for input_file in input_files:
            for dds_file in input_file.dds_files:
                dds_files.append({
                    "key": dds_file.file_id,
                    "dest": dds_file.destination_path
                })
        config_data = {"files": dds_files}
        payload = {
            "commands": json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=self.stage_job_name, data=payload)
        persistent_claim_volume = PersistentClaimVolume(self.job_claim_name,
                                                        mount_path="/data",
                                                        volume_claim_name=self.job_claim_name)

        stage_data_config_volume = ConfigMapVolume("config", mount_path="/etc/config",
                                                   config_map_name=self.stage_job_name,
                                                   source_key="commands", source_path="commands")
        ddsclient_secret_volume = SecretVolume(self.ddsclient_agent_name, mount_path="/etc/ddsclient",
                                               secret_name=self.ddsclient_agent_name)
        # Run job to stage data based on the config map
        container = Container(
            name=self.stage_job_name,
            image_name="jbradley/duke-ds-staging",
            command="python",
            args=["/app/download.py", "/etc/config/commands"],
            working_dir="/data",
            env_dict={"DDSCLIENT_CONF": "/etc/ddsclient/config"},
            requested_cpu="100m",
            requested_memory="64Mi",
            volumes=[
                persistent_claim_volume,
                ddsclient_secret_volume,
                stage_data_config_volume,
            ])
        job_spec = BatchJobSpec(self.stage_job_name, container=container)
        self.cluster_api.create_job(self.stage_job_name, job_spec)

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
