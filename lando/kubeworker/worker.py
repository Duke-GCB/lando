from lando.server.lando import JobApi, JobStates, JobSteps
from lando_messaging.clients import LandoClient, JobStepStoreOutputCompletePayload, JobCommands
from lando.kube.cluster import ClusterApi, BatchJobSpec, SecretVolume, PersistentClaimVolume, \
    ConfigMapVolume, Container, SecretEnvVar
import logging
import urllib
import os
import json


class JobStoreOutputPayload(object):
    def __init__(self, job_id, vm_instance_name):
        self.job_id = job_id
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.STORE_JOB_OUTPUT_COMPLETE


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
        self.output_project_name = 'Bespin-job-{}-results'.format(config.job_id)
        self.run_job_name = "run-job-{}".format(config.job_id)
        self.save_output_job_name = "save-output-job-{}".format(config.job_id)
        self.ddsclient_agent_name = "ddsclient-agent"
        self.job_claim_name = "job-{}-volume".format(config.job_id)
        self.lando_client = LandoClient(config, config.work_queue_config.listen_queue)

    def get_persistent_volume_claim(self):
        return PersistentClaimVolume(self.job_claim_name,
                                     mount_path="/data",
                                     volume_claim_name=self.job_claim_name)

    def run(self):
        if self.job.state == JobStates.STARTING:
            self.start_job()
        elif self.job.state == JobStates.RUNNING:
            self.restart_job()
        else:
            logging.error("Invalid job state {}".format())

    def start_job(self):
        self.job_api.set_job_state(JobStates.RUNNING)

        self.stage_data()
        self.run_workflow()
        self.store_output()

        output_project_info = None  # TODO send output project info
        self.lando_client.job_step_store_output_complete(
            JobStoreOutputPayload(self.job.id, self.job.vm_instance_name),
            output_project_info)

    def restart_job(self):
        raise NotImplemented("TODO")

    def add_workflow_to_volume(self):
        self.workflow.download_workflow()
        self.workflow.write_job_order_file()

    def stage_data(self):
        self.job_api.set_job_step(JobSteps.STAGING)
        self.add_workflow_to_volume()

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
                self.get_persistent_volume_claim(),
                ddsclient_secret_volume,
                stage_data_config_volume,
            ])
        job_spec = BatchJobSpec(self.stage_job_name, container=container)
        self.cluster_api.create_job(self.stage_job_name, job_spec)
        self.cluster_api.wait_for_jobs(job_names=[self.stage_job_name])
        self.cluster_api.delete_job(self.stage_job_name)
        self.cluster_api.delete_config_map(self.stage_job_name)

    def run_workflow(self):
        self.job_api.set_job_step(JobSteps.RUNNING)
        # Run job to stage data based on the config map
        base_cwl_command_ary = self.job.vm_settings.cwl_commands.base_command
        command = base_cwl_command_ary[0]
        workflow_filename = os.path.basename(self.job.workflow.url)
        args = ["--outdir", "data/results", "{}#main".format(workflow_filename), "job-order.json"]
        args.extend(base_cwl_command_ary[1:])
        container = Container(
            name=self.stage_job_name,
            image_name=self.job.vm_settings.image_name,
            command=command,
            args=args,
            working_dir="/data",
            env_dict={},
            requested_cpu="100m",
            requested_memory="64Mi",
            volumes=[
                self.get_persistent_volume_claim()
            ])
        job_spec = BatchJobSpec(self.run_job_name, container=container)
        self.cluster_api.create_job(self.run_job_name, job_spec)
        self.cluster_api.wait_for_jobs(job_names=[self.run_job_name])
        self.cluster_api.delete_job(self.run_job_name)

    def store_output(self):
        self.job_api.set_job_step(JobSteps.STORING_JOB_OUTPUT)
        config_data = {
            "destination": self.output_project_name,
            "paths": ["/data/results"]
        }
        payload = {
            "commands": json.dumps(config_data)
        }
        self.cluster_api.create_config_map(name=self.save_output_job_name, data=payload)
        save_results_config_volume = ConfigMapVolume("config", mount_path="/etc/config",
                                                     config_map_name=self.save_output_job_name,
                                                     source_key="commands", source_path="commands")
        ddsclient_secret_volume = SecretVolume(self.ddsclient_agent_name, mount_path="/etc/ddsclient",
                                               secret_name=self.ddsclient_agent_name)
        container = Container(
            name=self.save_output_job_name,
            image_name="jbradley/duke-ds-staging",
            command="python",
            args=["/app/upload.py", "/etc/config/commands"],
            working_dir="/data",
            env_dict={"DDSCLIENT_CONF": "/etc/ddsclient/config"},
            requested_cpu="100m",
            requested_memory="64Mi",
            volumes=[ddsclient_secret_volume, self.get_persistent_volume_claim(), save_results_config_volume])
        job_spec = BatchJobSpec(self.save_output_job_name, container=container)
        self.cluster_api.create_job(self.save_output_job_name, job_spec)

        # After stage job finishes cleanup
        self.cluster_api.wait_for_jobs(job_names=[self.save_output_job_name])
        self.cluster_api.delete_job(self.save_output_job_name)
        self.cluster_api.delete_config_map(self.save_output_job_name)


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
